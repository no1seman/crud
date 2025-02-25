local vshard = require('vshard')
local errors = require('errors')

local BucketIDError = errors.new_class("BucketIDError", {capture_stack = false})
local GetReplicasetsError = errors.new_class('GetReplicasetsError', {capture_stack = false})

local const = require('crud.common.const')
local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')
local sharding_metadata_module = require('crud.common.sharding.sharding_metadata')
local storage_metadata_cache = require('crud.common.sharding.storage_metadata_cache')
local sharding_utils = require('crud.common.sharding.utils')

local sharding = {}

function sharding.get_replicasets_by_bucket_id(bucket_id)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, GetReplicasetsError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
    end

    return {
        [replicaset.uuid] = replicaset,
    }
end

function sharding.key_get_bucket_id(space_name, key, specified_bucket_id)
    dev_checks('string', '?', '?number|cdata')

    if specified_bucket_id ~= nil then
        return { bucket_id = specified_bucket_id }
    end

    local sharding_func_data, err = sharding_metadata_module.fetch_sharding_func_on_router(space_name)
    if err ~= nil then
        return nil, err
    end

    if sharding_func_data.value ~= nil then
        return {
            bucket_id = sharding_func_data.value(key),
            sharding_func_hash = sharding_func_data.hash,
        }
    end

    return { bucket_id = vshard.router.bucket_id_strcrc32(key) }
end

function sharding.tuple_get_bucket_id(tuple, space, specified_bucket_id)
    if specified_bucket_id ~= nil then
        return { bucket_id = specified_bucket_id }
    end

    local sharding_index_parts = space.index[0].parts
    local sharding_key_data, err = sharding_metadata_module.fetch_sharding_key_on_router(space.name)
    if err ~= nil then
        return nil, err
    end
    if sharding_key_data.value ~= nil then
        sharding_index_parts = sharding_key_data.value.parts
    end
    local key = utils.extract_key(tuple, sharding_index_parts)

    local bucket_id_data, err = sharding.key_get_bucket_id(space.name, key, nil)
    if err ~= nil then
        return nil, err
    end

    return {
        bucket_id = bucket_id_data.bucket_id,
        sharding_func_hash = bucket_id_data.sharding_func_hash,
        sharding_key_hash = sharding_key_data.hash
    }
end

function sharding.tuple_set_and_return_bucket_id(tuple, space, specified_bucket_id)
    local bucket_id_fieldno, err = utils.get_bucket_id_fieldno(space)
    if err ~= nil then
        return nil, BucketIDError:new("Failed to get bucket ID fieldno: %s", err)
    end

    if specified_bucket_id ~= nil then
        if tuple[bucket_id_fieldno] == nil then
            tuple[bucket_id_fieldno] = specified_bucket_id
        else
            if tuple[bucket_id_fieldno] ~= specified_bucket_id then
                return nil, BucketIDError:new(
                    "Tuple and opts.bucket_id contain different bucket_id values: %s and %s",
                    tuple[bucket_id_fieldno], specified_bucket_id
                )
            end
        end
    end

    local sharding_data = { bucket_id = tuple[bucket_id_fieldno] }

    if sharding_data.bucket_id == nil then
        sharding_data, err = sharding.tuple_get_bucket_id(tuple, space)
        if err ~= nil then
            return nil, err
        end
        tuple[bucket_id_fieldno] = sharding_data.bucket_id
    else
        sharding_data.skip_sharding_hash_check = true
    end

    return sharding_data
end

function sharding.check_sharding_hash(space_name, sharding_func_hash, sharding_key_hash, skip_sharding_hash_check)
    if skip_sharding_hash_check == true then
        return true
    end

    local storage_func_hash = storage_metadata_cache.get_sharding_func_hash(space_name)
    local storage_key_hash = storage_metadata_cache.get_sharding_key_hash(space_name)

    if storage_func_hash ~= sharding_func_hash or storage_key_hash ~= sharding_key_hash then
        local err_msg = ('crud: Sharding hash mismatch for space %s. ' ..
                         'Sharding info will be refreshed after receiving this error. ' ..
                         'Please retry your request.'
                        ):format(space_name)
        return nil, sharding_utils.ShardingHashMismatchError:new(err_msg)
    end

    return true
end

function sharding.result_needs_sharding_reload(err)
    return err.class_name == sharding_utils.ShardingHashMismatchError.name
end

function sharding.wrap_method(method, space_name, ...)
    local i = 0

    local res, err, need_reload
    while true do
        res, err, need_reload = method(space_name, ...)

        if err == nil or need_reload ~= const.NEED_SHARDING_RELOAD then
            break
        end

        sharding_metadata_module.reload_sharding_cache(space_name)

        i = i + 1

        if i > const.SHARDING_RELOAD_RETRIES_NUM then
            break
        end
    end

    return res, err, need_reload
end

-- This wrapper assumes reload is performed inside the method and
-- expect ShardingHashMismatchError error to be thrown.
function sharding.wrap_select_method(method, space_name, ...)
    local i = 0

    local ok, res, err
    while true do
        ok, res, err = pcall(method, space_name, ...)

        if ok == true then
            break
        end

        -- Error thrown from merger casted to string,
        -- so the only way to identify it is string.find.
        local str_err = tostring(res)
        if (str_err:find(sharding_utils.ShardingHashMismatchError.name) == nil) then
            error(res)
        end

        -- Reload is performed inside the merger.

        i = i + 1

        if i > const.SHARDING_RELOAD_RETRIES_NUM then
            error(res)
        end
    end

    return res, err
end

return sharding
