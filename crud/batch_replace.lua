local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')

local BatchInsertIterator = require('crud.common.map_call_cases.batch_insert_iter')
local BatchPostprocessor = require('crud.common.map_call_cases.batch_postprocessor')

local BatchReplaceError = errors.new_class('BatchReplaceError', {capture_stack = false})

local batch_replace = {}

local BATCH_REPLACE_FUNC_NAME = '_crud.batch_replace_on_storage'

local function batch_replace_on_storage(space_name, tuples, opts)
    dev_checks('string', 'table', {
        fields = '?table',
        stop_on_error = '?boolean',
        rollback_on_error = '?boolean',
    })

    opts = opts or {}

    local space = box.space[space_name]
    if space == nil then
        return nil, {BatchReplaceError:new("Space %q doesn't exist", space_name)}
    end

    local inserted_tuples = {}
    local errs = {}

    box.begin()
    for _, tuple in ipairs(tuples) do
        local insert_result = schema.wrap_box_space_func_result(space, 'replace', {tuple}, {
            field_names = opts.fields,
        })

        if insert_result.err ~= nil then
            local err = {
                err = insert_result.err,
                tuple = tuple,
            }

            if opts.stop_on_error == true then
                if opts.rollback_on_error == true then
                    box.rollback()
                    return nil, {err}
                end

                box.commit()

                return inserted_tuples, {err}
            end

            table.insert(errs, err)
        end

        table.insert(inserted_tuples, insert_result.res)
    end

    if next(errs) ~= nil then
        if opts.rollback_on_error == true then
            box.rollback()
            return nil, errs
        end

        box.commit()

        return inserted_tuples, errs
    end

    box.commit()

    return inserted_tuples
end

function batch_replace.init()
    _G._crud.batch_replace_on_storage = batch_replace_on_storage
end

-- returns result, err, need_reload
-- need_reload indicates if reloading schema could help
-- see crud.common.schema.wrap_func_reload()
local function call_batch_replace_on_router(space_name, tuples, opts)
    dev_checks('string', 'table', {
        timeout = '?number',
        fields = '?table',
        stop_on_error = '?boolean',
        rollback_on_error = '?boolean',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, {BatchReplaceError:new("Space %q doesn't exist", space_name)}, true
    end

    local batch_replace_on_storage_opts = {
        fields = opts.fields,
        stop_on_error = opts.stop_on_error,
        rollback_on_error = opts.rollback_on_error,
    }

    local iter, err = BatchInsertIterator:new({
        tuples = tuples,
        space = space,
        execute_on_storage_opts = batch_replace_on_storage_opts,
    })
    if err ~= nil then
        return nil, {err}
    end

    local postprocessor = BatchPostprocessor:new()

    local rows, errs = call.map(BATCH_REPLACE_FUNC_NAME, nil, {
        timeout = opts.timeout,
        mode = 'write',
        iter = iter,
        postprocessor = postprocessor,
    })

    if next(rows) == nil then
        return nil, errs
    end

    local res, err = utils.format_result(rows, space, opts.fields)
    if err ~= nil then
        return nil, {err}
    end

    return res, errs
end

--- Batch replace tuples to the specified space
--
-- @function tuples_batch
--
-- @param string space_name
--  A space name
--
-- @param table tuples
--  Tuples
--
-- @tparam ?table opts
--  Options of batch_replace.tuples_batch
--
-- @return[1] tuples
-- @treturn[2] nil
-- @treturn[2] table of tables Error description

function batch_replace.tuples_batch(space_name, tuples, opts)
    checks('string', 'table', {
        timeout = '?number',
        fields = '?table',
        stop_on_error = '?boolean',
        rollback_on_error = '?boolean',
    })

    return schema.wrap_func_reload(call_batch_replace_on_router, space_name, tuples, opts)
end

--- Batch replace objects to the specified space
--
-- @function objects_batch
--
-- @param string space_name
--  A space name
--
-- @param table objs
--  Objects
--
-- @tparam ?table opts
--  Options of batch_insert.tuples_batch
--
-- @return[1] objects
-- @treturn[2] nil
-- @treturn[2] table of tables Error description

function batch_replace.objects_batch(space_name, objs, opts)
    checks('string', 'table', {
        timeout = '?number',
        fields = '?table',
        stop_on_error = '?boolean',
        rollback_on_error = '?boolean',
    })

    opts = opts or {}

    local tuples = {}
    local errs = {}

    for _, obj in ipairs(objs) do

        local tuple, err = utils.flatten_obj_reload(space_name, obj)
        if err ~= nil then
            local err_obj = BatchReplaceError:new("Failed to flatten object: %s", err)
            err_obj.tuple = obj

            if opts.stop_on_error == true then
                return nil, {err_obj}
            end

            table.insert(errs, err_obj)
        end

        table.insert(tuples, tuple)
    end

    if next(errs) ~= nil then
        return nil, errs
    end

    return batch_replace.tuples_batch(space_name, tuples, opts)
end

return batch_replace
