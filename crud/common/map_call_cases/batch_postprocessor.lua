local dev_checks = require('crud.common.dev_checks')
local utils = require('crud.common.utils')

local BasePostprocessor = require('crud.common.map_call_cases.base_postprocessor')

local BatchPostprocessor = {}
setmetatable(BatchPostprocessor, {__index = BasePostprocessor})

function BatchPostprocessor:collect(result_info, err_info)
    dev_checks('table', {
        key = '?',
        value = '?',
    },{
        err_wrapper = 'function|table',
        err = '?table|cdata',
        wrapper_args = '?table',
    })

    local errs = {err_info.err}
    if err_info.err == nil then
        errs = result_info.value[2]
    end

    if errs ~= nil then
        for _, err in pairs(errs) do
            local err_obj = err_info.err_wrapper(err.err or err, unpack(err_info.wrapper_args))
            err_obj.tuple = err.tuple

            self.errs = self.errs or {}
            table.insert(self.errs, err_obj)
        end
    end

    if result_info.value ~= nil and result_info.value[1] ~= nil then
        self.results = utils.table_extend(self.results, result_info.value[1])
    end

    return self.early_exit
end

return BatchPostprocessor
