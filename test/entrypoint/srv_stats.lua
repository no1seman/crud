#!/usr/bin/env tarantool

require('strict').on()
_G.is_initialized = function() return false end

local log = require('log')
local errors = require('errors')
local cartridge = require('cartridge')

package.preload['customers-storage'] = function()
    local engine = os.getenv('ENGINE') or 'memtx'
    return {
        role_name = 'customers-storage',
        init = function()
            local customers_space = box.schema.space.create('customers', {
                format = {
                    {name = 'id', type = 'unsigned'},
                    {name = 'bucket_id', type = 'unsigned'},
                    {name = 'name', type = 'string'},
                    {name = 'last_name', type = 'string'},
                    {name = 'age', type = 'number'},
                    {name = 'city', type = 'string'},
                },
                if_not_exists = true,
                engine = engine,
                id = 542,
            })
            -- primary index
            customers_space:create_index('id_index', {
                parts = { {field = 'id'} },
                if_not_exists = true,
            })
            customers_space:create_index('bucket_id', {
                parts = { {field = 'bucket_id'} },
                unique = false,
                if_not_exists = true,
            })
            customers_space:create_index('age_index', {
                parts = { {field = 'age'} },
                unique = false,
                if_not_exists = true,
            })
        end,
    }
end

local ok, err = errors.pcall('CartridgeCfgError', cartridge.cfg, {
    advertise_uri = 'localhost:3301',
    http_port = 8081,
    bucket_count = 3000,
    roles = {
        'cartridge.roles.crud-router',
        'cartridge.roles.crud-storage',
        'customers-storage',
    },
    roles_reload_allowed = true,
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end

_G.is_initialized = cartridge.is_healthy
