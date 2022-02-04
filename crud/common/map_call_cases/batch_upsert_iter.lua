local errors = require('errors')

local dev_checks = require('crud.common.dev_checks')
local sharding = require('crud.common.sharding')

local BaseIterator = require('crud.common.map_call_cases.base_iter')

local SplitTuplesError = errors.new_class('CallError')

local BatchUpsertIterator = {}
setmetatable(BatchUpsertIterator, {__index = BaseIterator})

function BatchUpsertIterator:new(opts)
    dev_checks('table', {
        tuples = 'table',
        space = 'table',
        operations = 'table',
        execute_on_storage_opts = 'table',
    })

    local batches_by_replicasets, err = sharding.split_tuples_by_replicaset(opts.tuples, opts.space, {
        operations = opts.operations,
    })
    if err ~= nil then
        return nil, SplitTuplesError:new("Failed to split tuples by replicaset: %s", err.err)
    end

    local next_replicaset, next_batch = next(batches_by_replicasets)

    local iter = {
        space_name = opts.space.name,
        opts = opts.execute_on_storage_opts,
        batches_by_replicasets = batches_by_replicasets,
        next_index = next_replicaset,
        next_batch = next_batch,
    }

    setmetatable(iter, self)
    self.__index = self

    return iter
end

function BatchUpsertIterator:get()
    local replicaset = self.next_index
    local func_args = {
        self.space_name,
        self.next_batch.tuples,
        self.next_batch.operations,
        self.opts,
    }

    self.next_index, self.next_batch = next(self.batches_by_replicasets, self.next_index)

    return func_args, replicaset
end

return BatchUpsertIterator
