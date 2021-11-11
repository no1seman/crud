local errors = require('errors')
local vshard = require('vshard')

local dev_checks = require('crud.common.dev_checks')
local GetReplicasetsError = errors.new_class('CallError')

local BaseIterator = {}

function BaseIterator:new(opts)
    dev_checks('table', {
        func_args = '?table',
        replicasets = '?table',
    })

    local replicasets, err
    if opts.replicasets ~= nil then
        replicasets = opts.replicasets
    else
        replicasets, err = vshard.router.routeall()
        if replicasets == nil then
            return nil, GetReplicasetsError:new("Failed to get all replicasets: %s", err.err)
        end
    end

    local next_index, next_replicaset = next(replicasets)

    local iter = {
        func_args = opts.func_args,
        replicasets = replicasets,
        next_replicaset = next_replicaset,
        next_index = next_index
    }

    setmetatable(iter, self)
    self.__index = self

    return iter
end

function BaseIterator:has_next()
    return self.next_index ~= nil
end

function BaseIterator:get()
    local replicaset = self.next_replicaset
    self.next_index, self.next_replicaset = next(self.replicasets, self.next_index)

    return self.func_args, replicaset
end

return BaseIterator
