local ratelimit = require('crud.ratelimit')
local check_select_safety_rl = ratelimit.new()

local common = {}

common.SELECT_FUNC_NAME = '_crud.select_on_storage'
common.DEFAULT_BATCH_SIZE = 100

common.check_select_safety = function(space_name, user_conditions, opts)
    if opts.fullscan ~= nil and opts.fullscan == true then
        return
    end

    if opts.first ~= nil and math.abs(opts.first) <= 1000 then
        return
    end

    if user_conditions ~= nil then
        for _, v in ipairs(user_conditions) do
            local it = v[1]
            if it ~= nil and type(it) == 'string' and (it == '=' or it == '==') then
                return
            end
        end
    end

    local rl = check_select_safety_rl
    local traceback = debug.traceback()
    rl:log_crit("Potentially long select from space '%s'\n %s", space_name, traceback)
end

return common
