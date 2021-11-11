local dev_checks = require('crud.common.dev_checks')

local BasePostprocessor = {}

function BasePostprocessor:new()
    local postprocessor = {
        results = {},
        early_exit = false,
        errs = nil
    }

    setmetatable(postprocessor, self)
    self.__index = self

    return postprocessor
end

function BasePostprocessor:collect(result_info, err_info)
    dev_checks('table', {
        key = '?',
        value = '?',
    },{
        err_wrapper = 'function|table',
        err = '?table|cdata',
        wrapper_args = '?table',
    })

    local err = err_info.err
    if err == nil and result_info.value[1] == nil then
        err = result_info.value[2]
    end

    if err ~= nil then
        self.results = nil
        self.errs = err_info.err_wrapper(err, unpack(err_info.wrapper_args))
        self.early_exit = true

        return self.early_exit
    end

    if self.early_exit ~= true then
        self.results[result_info.key] = result_info.value
    end

    return self.early_exit
end

function BasePostprocessor:get()
    return self.results, self.errs
end

return BasePostprocessor
