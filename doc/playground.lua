#!/usr/bin/env tarantool

-- How to run:
--
-- $ ./doc/playground.lua
--
-- Or
--
-- $ KEEP_DATA=1 ./doc/playground.lua
--
-- What to do next:
--
-- Choose an example from doc/select.md or doc/pairs.md and run.
-- For example:
--
-- tarantool> crud.select('developers', nil, {first = 6})
--
-- You can also use help() and prev() functions to navigate over
-- examples in the documentation.

local fio = require('fio')
local vshard = require('vshard')
local crud = require('crud')
local console = require('console')

-- Trick to don't leave *.snap, *.xlog files. See
-- test/tuple_keydef.test.lua in the tuple-keydef module.
if os.getenv('KEEP_DATA') ~= nil then
    box.cfg()
else
    local tempdir = fio.tempdir()
    box.cfg({
        memtx_dir = tempdir,
        wal_mode = 'none',
    })
    fio.rmtree(tempdir)
end

-- Setup vshard.
_G.vshard = vshard
box.once('guest', function()
    box.schema.user.grant('guest', 'super')
end)
local uri = 'guest@localhost:3301'
local cfg = {
    bucket_count = 3000,
    sharding = {
        [box.info().cluster.uuid] = {
            replicas = {
                [box.info().uuid] = {
                    uri = uri,
                    name = 'storage',
                    master = true,
                },
            },
        },
    },
}
vshard.storage.cfg(cfg, box.info().uuid)
vshard.router.cfg(cfg)
vshard.router.bootstrap()

-- Create a space.
box.once('developers', function()
    box.schema.create_space('developers', {
        format = {
            {name = 'id', type = 'unsigned'},
            {name = 'bucket_id', type = 'unsigned'},
            {name = 'name', type = 'string'},
            {name = 'surname', type = 'string'},
            {name = 'age', type = 'number'},
        }
    })
    box.space.developers:create_index('primary_index', {
        parts = {
            {field = 1, type = 'unsigned'},
        },
    })
    box.space.developers:create_index('age_index', {
        parts = {
            {field = 5, type = 'number'},
        },
    })
    box.space.developers:create_index('full_name', {
        parts = {
            {field = 3, type = 'string'},
            {field = 4, type = 'string'},
        },
    })

    -- Fill the space.
    box.space.developers:insert({1, 7331, 'Alexey', 'Adams', 20})
    box.space.developers:insert({2, 899, 'Sergey', 'Allred', 21})
    box.space.developers:insert({3, 9661, 'Pavel', 'Adams', 27})
    box.space.developers:insert({4, 501, 'Mikhail', 'Liston', 51})
    box.space.developers:insert({5, 1993, 'Dmitry', 'Jacobi', 16})
    box.space.developers:insert({6, 8765, 'Alexey', 'Sidorov', 31})
end)

-- Initialize crud.
crud.init_storage()
crud.init_router()

-- {{{ help() and prev()

local examples = {}
local next_example = 1

local function add_help(filename)
    local fh = fio.open(filename)
    local doc = fh:read()
    fh:close()

    local example = {}

    local state = 'initial'
    for _, line in ipairs(doc:split('\n')) do
        if state == 'initial' then
            if line:startswith('```') then
                state = 'in_example'
            end
        elseif state == 'in_example' then
            if line:startswith('```') then
                table.insert(examples, table.concat(example, '\n'))
                example = {}
                state = 'initial'
            else
                table.insert(example, line)
            end
        else
            assert(false)
        end
    end
end

local function help()
    if next_example > #examples then
        print('No more examples')
        return
    end

    print(examples[next_example])
    next_example = next_example + 1
end

local function prev()
    if next_example - 2 < 1 then
        print("You're on the first example")
        return
    end

    print(examples[next_example - 2])
    next_example = next_example - 1
end

add_help('doc/select.md')
add_help('doc/pairs.md')
_G.help = help
_G.prev = prev

-- }}} help() and prev()

-- Start a console.
console.start()
os.exit()
