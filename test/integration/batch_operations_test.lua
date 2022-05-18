local fio = require('fio')

local t = require('luatest')
local crud = require('crud')

local helpers = require('test.helper')

local pgroup = t.group('batch_operations', {
    {engine = 'memtx'},
    {engine = 'vinyl'},
})

pgroup.before_all(function(g)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_batch_operations'),
        use_vshard = true,
        replicasets = helpers.get_test_replicasets(),
        env = {
            ['ENGINE'] = g.params.engine,
        },
    })

    g.cluster:start()
end)

pgroup.after_all(function(g) helpers.stop_cluster(g.cluster) end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

pgroup.test_non_existent_space = function(g)
    -- insert_many
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_many', {
        'non_existent_space',
        {
            {1, box.NULL, 'Alex', 59},
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18}
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')

    -- insert_object_many
    -- default: stop_on_error == false
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'non_existent_space',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)

    -- we got 3 errors about non existent space, because it caused by flattening objects
    t.assert_equals(#errs, 3)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')
    t.assert_str_contains(errs[2].err, 'Space "non_existent_space" doesn\'t exist')
    t.assert_str_contains(errs[3].err, 'Space "non_existent_space" doesn\'t exist')

    -- insert_object_many
    -- stop_on_error == true
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'non_existent_space',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)

    -- we got 1 errors about non existent space, because stop_on_error == true
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')

    -- upsert_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'non_existent_space',
        {
            {1, box.NULL, 'Alex', 59},
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18}
        },
        {
            {{'+', 'age', 1}},
            {{'+', 'age', 1}},
            {{'+', 'age', 1}},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')

    -- upsert_object_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'non_existent_space',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
        },
        {
            {{'+', 'age', 1}},
            {{'+', 'age', 1}},
            {{'+', 'age', 1}},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)

    -- we got 3 errors about non existent space, because it caused by flattening objects
    t.assert_equals(#errs, 3)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')
    t.assert_str_contains(errs[2].err, 'Space "non_existent_space" doesn\'t exist')
    t.assert_str_contains(errs[3].err, 'Space "non_existent_space" doesn\'t exist')

    -- upsert_object_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'non_existent_space',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
        },
        {
            {{'+', 'age', 1}},
            {{'+', 'age', 1}},
            {{'+', 'age', 1}},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)

    -- we got 1 errors about non existent space, because stop_on_error == true
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')

    -- replace_many
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'non_existent_space',
        {
            {1, box.NULL, 'Alex', 59},
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18}
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')

    -- replace_object_many
    -- default: stop_on_error == false
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'non_existent_space',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)

    -- we got 3 errors about non existent space, because it caused by flattening objects
    t.assert_equals(#errs, 3)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')
    t.assert_str_contains(errs[2].err, 'Space "non_existent_space" doesn\'t exist')
    t.assert_str_contains(errs[3].err, 'Space "non_existent_space" doesn\'t exist')

    -- replace_object_many
    -- stop_on_error == true
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'non_existent_space',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)

    -- we got 1 errors about non existent space, because stop_on_error == true
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')
end

pgroup.test_batch_insert_object_get = function(g)
    -- bad format
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna'},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Field \"age\" isn\'t nullable')
    t.assert_equals(errs[1].tuple, {id = 2, name = 'Anna'})

    -- bad format
    -- two errors, default: stop_on_error == false
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor'},
            {id = 2, name = 'Anna'},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple.id < err2.tuple.id end)

    t.assert_str_contains(errs[1].err, 'Field \"age\" isn\'t nullable')
    t.assert_equals(errs[1].tuple, {id = 1, name = 'Fedor'})

    t.assert_str_contains(errs[2].err, 'Field \"age\" isn\'t nullable')
    t.assert_equals(errs[2].tuple, {id = 2, name = 'Anna'})

    -- batch_insert_object
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 1, name = 'Fedor', age = 59, bucket_id = 477},
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
    })

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- batch_insert_object again
    -- default: stop_on_error = false, rollback_on_error = false
    -- one error on one storage without rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 22, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 5, name = 'Sergey', age = 25},
            {id = 9, name = 'Anna', age = 30},
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 9, name = 'Anna', age = 30, bucket_id = 1644},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
    })

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, {9, 1644, 'Anna', 30})

    -- batch_insert_object again
    -- fails for both: s1-master s2-master
    -- one error on each storage, one success on each storage
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 2, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 10, name = 'Sergey', age = 25},
            {id = 92, name = 'Artur', age = 29},
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 10, name = 'Sergey', age = 25, bucket_id = 569},
        {id = 92, name = 'Artur', age = 29, bucket_id = 2040},
    })

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, {10, 569, 'Sergey', 25})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})

    -- batch_insert_object again
    -- fails for both: s1-master s2-master
    -- no success
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 2, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 10, name = 'Vlad', age = 25},
            {id = 92, name = 'Mark', age = 29},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 4)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[3].err, 'Duplicate key exists')
    t.assert_equals(errs[3].tuple, {10, 569, 'Vlad', 25})

    t.assert_str_contains(errs[4].err, 'Duplicate key exists')
    t.assert_equals(errs[4].tuple, {92, 2040, 'Mark', 29})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, {10, 569, 'Sergey', 25})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})
end

pgroup.test_batch_insert_object_stop_on_error = function(g)
    -- bad format
    -- two errors, stop_on_error == true
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor'},
            {id = 2, name = 'Anna'},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)

    t.assert_str_contains(errs[1].err, 'Field \"age\" isn\'t nullable')
    t.assert_equals(errs[1].tuple, {id = 1, name = 'Fedor'})

    -- batch_insert_object
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18},
            {id = 71, name = 'Oleg', age = 32}
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
        {id = 71, name = 'Oleg', age = 32, bucket_id = 1802},
    })

    -- get
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})

    -- batch_insert_object again
    -- default: stop_on_error = true, rollback_on_error = false
    -- one error on one storage without rollback, inserts stop by error on this storage
    -- inserts before error are successful
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 22, name = 'Alex', age = 34},
            {id = 92, name = 'Artur', age = 29},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 5, name = 'Sergey', age = 25},
            {id = 9, name = 'Anna', age = 30},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
        {id = 92, name = 'Artur', age = 29, bucket_id = 2040},
    })

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- batch_insert_object again
    -- fails for both: s1-master s2-master
    -- one error on each storage, all inserts stop by error
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 2, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 10, name = 'Sergey', age = 25},
            {id = 9, name = 'Anna', age = 30},
            {id = 92, name = 'Leo', age = 29},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, nil)

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})

    -- batch_insert_object again
    -- fails for both: s1-master s2-master
    -- no success
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 2, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 71, name = 'Inga', age = 32},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})
end

pgroup.test_batch_insert_object_rollback_on_error = function(g)
    -- batch_insert_object
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18},
            {id = 71, name = 'Oleg', age = 32}
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
        {id = 71, name = 'Oleg', age = 32, bucket_id = 1802},
    })

    -- get
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})

    -- batch_insert_object again
    -- default: stop_on_error = true, rollback_on_error = false
    -- one error on one storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 22, name = 'Alex', age = 34},
            {id = 92, name = 'Artur', age = 29},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 5, name = 'Sergey', age = 25},
            {id = 9, name = 'Anna', age = 30},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
    })

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- batch_insert_object again
    -- fails for both: s1-master s2-master
    -- one error on each storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 10, name = 'Sergey', age = 25},
            {id = 9, name = 'Anna', age = 30},
            {id = 2, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- batch_insert_object again
    -- fails for both: s1-master s2-master
    -- two errors on each storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 1, name = 'Olga', age = 27},
            {id = 71, name = 'Sergey', age = 25},
            {id = 5, name = 'Anna', age = 30},
            {id = 2, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 4)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[3].err, 'Duplicate key exists')
    t.assert_equals(errs[3].tuple, {5, 1172, "Anna", 30})

    t.assert_str_contains(errs[4].err, 'Duplicate key exists')
    t.assert_equals(errs[4].tuple, {71, 1802, "Sergey", 25})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})
end

pgroup.test_batch_insert_object_rollback_and_stop_on_error = function(g)
    -- batch_insert_object
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18},
            {id = 71, name = 'Oleg', age = 32}
        },
        {
            stop_on_error = true,
            rollback_on_error  = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
        {id = 71, name = "Oleg", age = 32, bucket_id = 1802}
    })

    -- get
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})

    -- batch_insert_object again
    -- stop_on_error = true, rollback_on_error = true
    -- one error on one storage with rollback, inserts stop by error on this storage
    -- inserts before error are rollbacked
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 22, name = 'Alex', age = 34},
            {id = 92, name = 'Artur', age = 29},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 5, name = 'Sergey', age = 25},
            {id = 9, name = 'Anna', age = 30},
        },
        {
            stop_on_error = true,
            rollback_on_error  = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
    })

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)
end

pgroup.test_batch_insert_get = function(g)
    -- batch_insert_object
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_many', {
        'customers',
        {
            {1, box.NULL, 'Fedor', 59},
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18}
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 1, name = 'Fedor', age = 59, bucket_id = 477},
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
    })

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- batch_insert again
    -- default: stop_on_error = false, rollback_on_error = false
    -- one error on one storage without rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_many', {
        'customers',
        {
            {22, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
            {5, box.NULL, 'Sergey', 25},
            {9, box.NULL, 'Anna', 30},
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 9, name = 'Anna', age = 30, bucket_id = 1644},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
    })

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, {9, 1644, 'Anna', 30})

    -- batch_insert again
    -- fails for both: s1-master s2-master
    -- one error on each storage, one success on each storage
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_many', {
        'customers',
        {
            {2, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
            {10, box.NULL, 'Sergey', 25},
            {92, box.NULL, 'Artur', 29},
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 10, name = 'Sergey', age = 25, bucket_id = 569},
        {id = 92, name = 'Artur', age = 29, bucket_id = 2040},
    })

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, {10, 569, 'Sergey', 25})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})

    -- batch_insert again
    -- fails for both: s1-master s2-master
    -- no success
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_many', {
        'customers',
        {
            {2, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
            {10, box.NULL, 'Sergey', 25},
            {92, box.NULL, 'Artur', 29},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 4)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[3].err, 'Duplicate key exists')
    t.assert_equals(errs[3].tuple, {10, 569, 'Sergey', 25})

    t.assert_str_contains(errs[4].err, 'Duplicate key exists')
    t.assert_equals(errs[4].tuple, {92, 2040, 'Artur', 29})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, {10, 569, 'Sergey', 25})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})
end

pgroup.test_batch_insert_stop_on_error = function(g)
    -- batch_insert
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_many', {
        'customers',
        {
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18},
            {71, box.NULL, 'Oleg', 32}
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
        {id = 71, name = "Oleg", age = 32, bucket_id = 1802},
    })

    -- get
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})

    -- batch_insert again
    -- default: stop_on_error = true, rollback_on_error = false
    -- one error on one storage without rollback, inserts stop by error on this storage
    -- inserts before error are successful
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_many', {
        'customers',
        {
            {22, box.NULL, 'Alex', 34},
            {92, box.NULL, 'Artur', 29},
            {3, box.NULL, 'Anastasia', 22},
            {5, box.NULL, 'Sergey', 25},
            {9, box.NULL, 'Anna', 30},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
        {id = 92, name = 'Artur', age = 29, bucket_id = 2040},
    })

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- batch_insert again
    -- fails for both: s1-master s2-master
    -- one error on each storage, all inserts stop by error
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_many', {
        'customers',
        {
            {2, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
            {10, box.NULL, 'Sergey', 25},
            {9, box.NULL, 'Anna', 30},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- batch_insert_object again
    -- fails for both: s1-master s2-master
    -- no success
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_many', {
        'customers',
        {
            {2, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
            {71, box.NULL, 'Inga', 32},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})
end

pgroup.test_batch_insert_rollback_on_error = function(g)
    -- batch_insert_object
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_many', {
        'customers',
        {
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18},
            {71, box.NULL, 'Oleg', 32}
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
        {id = 71, name = 'Oleg', age = 32, bucket_id = 1802},
    })

    -- get
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})

    -- batch_insert_object again
    -- default: stop_on_error = true, rollback_on_error = false
    -- one error on one storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_many', {
        'customers',
        {
            {22, box.NULL, 'Alex', 34},
            {92, box.NULL, 'Artur', 29},
            {3, box.NULL, 'Anastasia', 22},
            {5, box.NULL, 'Sergey', 25},
            {9, box.NULL, 'Anna', 30},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
    })

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- batch_insert_object again
    -- fails for both: s1-master s2-master
    -- one error on each storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_many', {
        'customers',
        {
            {10, box.NULL, 'Sergey', 25},
            {9, box.NULL, 'Anna', 30},
            {2, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- batch_insert_object again
    -- fails for both: s1-master s2-master
    -- two errors on each storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_many', {
        'customers',
        {
            {1, box.NULL, 'Olga', 27},
            {71, box.NULL, 'Sergey', 25},
            {5, box.NULL, 'Anna', 30},
            {2, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 4)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[3].err, 'Duplicate key exists')
    t.assert_equals(errs[3].tuple, {5, 1172, "Anna", 30})

    t.assert_str_contains(errs[4].err, 'Duplicate key exists')
    t.assert_equals(errs[4].tuple, {71, 1802, "Sergey", 25})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})
end

pgroup.test_batch_insert_rollback_and_stop_on_error = function(g)
    -- batch_insert
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_many', {
        'customers',
        {
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18},
            {71, box.NULL, 'Oleg', 32}
        },
        {
            stop_on_error = true,
            rollback_on_error  = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
        {id = 71, name = "Oleg", age = 32, bucket_id = 1802}
    })

    -- get
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})

    -- batch_insert again
    -- stop_on_error = true, rollback_on_error = true
    -- one error on one storage with rollback, inserts stop by error on this storage
    -- inserts before error are rollbacked
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_many', {
        'customers',
        {
            {22, box.NULL, 'Alex', 34},
            {92, box.NULL, 'Artur', 29},
            {3, box.NULL, 'Anastasia', 22},
            {5, box.NULL, 'Sergey', 25},
            {9, box.NULL, 'Anna', 30},
        },
        {
            stop_on_error = true,
            rollback_on_error  = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
    })

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)
end

pgroup.test_upsert_object_many_get = function(g)
    -- bad format
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna'},
            {id = 3, name = 'Inga'},
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 'Leo Tolstoy'},},
            {{'+', 'age', 12}}
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    t.assert_str_contains(errs[1].err, 'Field \"age\" isn\'t nullable')
    t.assert_equals(errs[1].tuple, {id = 2, name = 'Anna'})

    t.assert_str_contains(errs[2].err, 'Field \"age\" isn\'t nullable')
    t.assert_equals(errs[2].tuple, {id = 3, name = 'Inga'})

    -- upsert_object_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 'Leo Tolstoy'},},
            {{'+', 'age', 12}},
            {{'=', 'name', 'Jane'}},
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}, {}})

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- upsert_object_many again
    -- success with updating one record
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 1, name = 'Alex', age = 34},
            {id = 81, name = 'Anastasia', age = 22},
            {id = 92, name = 'Sergey', age = 25},
        },
        {
            {{'+', 'age', 10}, {'=', 'name', 'Leo Tolstoy'},},
            {{'+', 'age', 12}},
            {{'=', 'name', 'Pavel'}}
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}, {}})

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Leo Tolstoy', 69})

    -- primary key = 81 -> bucket_id = 2205 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(81)
    t.assert_equals(result, {81, 2205, 'Anastasia', 22})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Sergey', 25})

    -- upsert_object_many again
    -- success with updating all records
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 1, name = 'Alex', age = 34},
            {id = 81, name = 'Anastasia', age = 21},
            {id = 92, name = 'Sergey', age = 24},
        },
        {
            {{'+', 'age', 1}, {'=', 'name', 'Peter'},},
            {{'+', 'age', 5}},
            {{'=', 'name', 'Pavel'}}
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}, {}})

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Peter', 70})

    -- primary key = 81 -> bucket_id = 2205 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(81)
    t.assert_equals(result, {81, 2205, 'Anastasia', 27})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Pavel', 25})

    -- upsert_object_many again
    -- failed for s1-master
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 22, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 5, name = 'Sergey', age = 25},
        },
        {
            {{'=', 'name', 'Peter'},},
            {{'=', 'age', 'invalid type'}, {'=', 'name', 'Leo Tolstoy'},},
            {{'=', 'age', 5}},
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[1].err, 'Tuple field 4 (age) type does not match one required by operation')
    else
        t.assert_str_contains(errs[1].err, 'Tuple field 4 type does not match one required by operation')
    end
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}})

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- upsert_object_many again
    -- fails for both: s1-master s2-master
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 2, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 10, name = 'Sergey', age = 25},
        },
        {
            {{'=', 'age', 'invalid type'}, {'=', 'name', 'Leo Tolstoy'},},
            {{'=', 'name', 5},},
            {{'=', 'age', 5}}
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[1].err, 'Tuple field 4 (age) type does not match one required by operation')
    else
        t.assert_str_contains(errs[1].err, 'Tuple field 4 type does not match one required by operation')
    end
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[2].err, 'Tuple field 3 (name) type does not match one required by operation')
    else
        t.assert_str_contains(errs[2].err, 'Tuple field 3 type does not match one required by operation')
    end
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, {10, 569, 'Sergey', 25})
end

pgroup.test_upsert_object_many_stop_on_error = function(g)
    -- bad format
    -- two errors
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna'},
            {id = 3, name = 'Peter'},
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 'Leo Tolstoy'},},
            {{'+', 'age', 12}}
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Field \"age\" isn\'t nullable')
    t.assert_equals(errs[1].tuple, {id = 2, name = 'Anna'})

    -- upsert_object_many
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 'Leo Tolstoy'},},
            {{'+', 'age', 12}},
            {{'=', 'name', 'Jane'}},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}, {}})

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- upsert_object_many again
    -- default: stop_on_error = true, rollback_on_error = false
    -- one error on one storage without rollback, inserts stop by error on this storage
    -- inserts before error are successful
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 22, name = 'Alex', age = 34},
            {id = 92, name = 'Artur', age = 29},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 5, name = 'Sergey', age = 25},
            {id = 9, name = 'Anna', age = 30},
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 'Leo Tolstoy'},},
            {{'+', 'age', 12}},
            {{'=', 'name', 5}},
            {{'+', 'age', 1}},
            {{'+', 'age', 2}},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[1].err, 'Tuple field 3 (name) type does not match one required by operation')
    else
        t.assert_str_contains(errs[1].err, 'Tuple field 3 type does not match one required by operation')
    end
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}, {}})

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- upsert_object_many again
    -- fails for both: s1-master s2-master
    -- one error on each storage, all inserts stop by error
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 2, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 10, name = 'Sergey', age = 25},
            {id = 9, name = 'Anna', age = 30},
            {id = 92, name = 'Leo', age = 29},
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 5},},
            {{'+', 'age', 'invalid'}},
            {{'=', 'name', 'Pavel'}},
            {{'+', 'age', 1}},
            {{'+', 'age', 2}},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[1].err, 'Tuple field 3 (name) type does not match one required by operation')
    else
        t.assert_str_contains(errs[1].err, 'Tuple field 3 type does not match one required by operation')
    end
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, nil)

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})
end

pgroup.test_upsert_object_many_rollback_on_error = function(g)
    -- upsert_object_many
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18},
            {id = 71, name = 'Oleg', age = 32}
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 'Inga'},},
            {{'+', 'age', 1}},
            {{'+', 'age', 2}},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}, {}})

    -- get
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})

    -- upsert_object_many again
    -- one error on one storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 22, name = 'Alex', age = 34},
            {id = 92, name = 'Artur', age = 29},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 5, name = 'Sergey', age = 25},
            {id = 9, name = 'Anna', age = 30},
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 'Inga'},},
            {{'+', 'age', 1}},
            {{'+', 'age', 'invalid'}},
            {{'+', 'age', 1}},
            {{'+', 'age', 1}},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}})

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- upsert_object_many again
    -- fails for both: s1-master s2-master
    -- one error on each storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 10, name = 'Sergey', age = 25},
            {id = 9, name = 'Anna', age = 30},
            {id = 2, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 'Inga'},},
            {{'+', 'age', 1}},
            {{'+', 'age', 'invalid'}},
            {{'+', 'age', 'invalid'}},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- upsert_object_many again
    -- fails for both: s1-master s2-master
    -- two errors on each storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 1, name = 'Olga', age = 27},
            {id = 71, name = 'Sergey', age = 25},
            {id = 5, name = 'Anna', age = 30},
            {id = 2, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
        },
        {
            {{'+', 'age', 1}},
            {{'+', 'age', 'invalid'}},
            {{'+', 'age', 'invalid'}},
            {{'+', 'age', 'invalid'}},
            {{'+', 'age', 'invalid'}},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 4)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[3].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[3].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[3].tuple, {5, 1172, "Anna", 30})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[4].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[4].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[4].tuple, {71, 1802, "Sergey", 25})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})
end

pgroup.test_upsert_object_many_rollback_and_stop_on_error = function(g)
    -- upsert_object_many
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18},
            {id = 71, name = 'Oleg', age = 32}
        },
        {
            {{'+', 'age', 1}},
            {{'+', 'age', 1}},
            {{'+', 'age', 1}},
        },
        {
            stop_on_error = true,
            rollback_on_error  = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}, {}})

    -- get
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})

    -- upsert_object_many again
    -- stop_on_error = true, rollback_on_error = true
    -- one error on one storage with rollback, inserts stop by error on this storage
    -- inserts before error are rollbacked
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 22, name = 'Alex', age = 34},
            {id = 92, name = 'Artur', age = 29},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 5, name = 'Sergey', age = 25},
            {id = 9, name = 'Anna', age = 30},
        },
        {
            {{'+', 'age', 1}},
            {{'+', 'age', 1}},
            {{'+', 'age', 'invalid'}},
            {{'+', 'age', 1}},
            {{'+', 'age', 1}},
        },
        {
            stop_on_error = true,
            rollback_on_error  = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}})

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)
end

pgroup.test_upsert_many_get = function(g)
    -- upsert_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {1, box.NULL, 'Fedor', 59},
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18}
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 'Leo Tolstoy'},},
            {{'+', 'age', 20},},
            {{'=', 'name', 'Jane'}}
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}, {}})

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- upsert_many again
    -- success with updating one record
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {1, box.NULL, 'Alex', 34},
            {81, box.NULL, 'Anastasia', 21},
            {92, box.NULL, 'Sergey', 24},
        },
        {
            {{'+', 'age', 10}, {'=', 'name', 'Leo Tolstoy'},},
            {{'+', 'age', 5},},
            {{'=', 'name', 'Pavel'}},
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}, {}})

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Leo Tolstoy', 69})

    -- primary key = 81 -> bucket_id = 2205 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(81)
    t.assert_equals(result, {81, 2205, 'Anastasia', 21})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Sergey', 24})

    -- batch_insert again
    -- success with updating all records
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {1, box.NULL, 'Alex', 34},
            {81, box.NULL, 'Anastasia', 22},
            {92, box.NULL, 'Sergey', 25},
        },
        {
            {{'+', 'age', 1}, {'=', 'name', 'Peter'}},
            {{'+', 'age', 5}},
            {{'=', 'name', 'Pavel'}}
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}, {}})

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Peter', 70})

    -- primary key = 81 -> bucket_id = 2205 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(81)
    t.assert_equals(result, {81, 2205, 'Anastasia', 26})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Pavel', 24})

    -- upsert_many again
    -- failed for s1-master
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {22, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
            {5, box.NULL, 'Sergey', 25},
        },
        {
            {{'=', 'age', 64}, {'=', 'name', 'Leo Tolstoy'}},
            {{'=', 'age', 'invalid type'}},
            {{'=', 'age', 4}}
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[1].err, 'Tuple field 4 (age) type does not match one required by operation')
    else
        t.assert_str_contains(errs[1].err, 'Tuple field 4 type does not match one required by operation')
    end
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}})

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- upsert_many again
    -- fails for both: s1-master s2-master
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {2, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
            {10, box.NULL, 'Sergey', 25},
        },
        {
            {{'=', 'age', 'invalid type'}, {'=', 'name', 'Leo Tolstoy'}},
            {{'=', 'name', 5}},
            {{'=', 'name', 'Pavel'}}
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[1].err, 'Tuple field 4 (age) type does not match one required by operation')
    else
        t.assert_str_contains(errs[1].err, 'Tuple field 4 type does not match one required by operation')
    end
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[2].err, 'Tuple field 3 (name) type does not match one required by operation')
    else
        t.assert_str_contains(errs[2].err, 'Tuple field 3 type does not match one required by operation')
    end
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, {10, 569, 'Sergey', 25})
end

pgroup.test_upsert_many_stop_on_error = function(g)
    -- upsert_many
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {1, box.NULL, 'Fedor', 59},
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18}
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 'Leo Tolstoy'},},
            {{'+', 'age', 12}},
            {{'=', 'name', 'Jane'}},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}, {}})

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- upsert_many again
    -- one error on one storage without rollback, inserts stop by error on this storage
    -- inserts before error are successful
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {22, box.NULL, 'Alex', 34},
            {92, box.NULL, 'Artur', 29},
            {3, box.NULL, 'Anastasia', 22},
            {5, box.NULL, 'Sergey', 25},
            {9, box.NULL, 'Anna', 30},
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 'Leo Tolstoy'},},
            {{'+', 'age', 12}},
            {{'=', 'name', 5}},
            {{'+', 'age', 1}},
            {{'+', 'age', 2}},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[1].err, 'Tuple field 3 (name) type does not match one required by operation')
    else
        t.assert_str_contains(errs[1].err, 'Tuple field 3 type does not match one required by operation')
    end
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}, {}})

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- upsert_object_many again
    -- fails for both: s1-master s2-master
    -- one error on each storage, all inserts stop by error
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {2, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
            {10, box.NULL, 'Sergey', 25},
            {9, box.NULL, 'Anna', 30},
            {92, box.NULL, 'Leo', 29},
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 5},},
            {{'+', 'age', 'invalid'}},
            {{'=', 'name', 'Pavel'}},
            {{'+', 'age', 1}},
            {{'+', 'age', 2}},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[1].err, 'Tuple field 3 (name) type does not match one required by operation')
    else
        t.assert_str_contains(errs[1].err, 'Tuple field 3 type does not match one required by operation')
    end
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, nil)

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})
end

pgroup.test_upsert_many_rollback_on_error = function(g)
    -- upsert_object_many
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18},
            {71, box.NULL, 'Oleg', 32}
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 'Inga'},},
            {{'+', 'age', 1}},
            {{'+', 'age', 2}},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}, {}})

    -- get
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})

    -- upsert_many again
    -- one error on one storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {22, box.NULL, 'Alex', 34},
            {92, box.NULL, 'Artur', 29},
            {3, box.NULL, 'Anastasia', 22},
            {5, box.NULL, 'Sergey', 25},
            {9, box.NULL, 'Anna', 30},
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 'Inga'},},
            {{'+', 'age', 1}},
            {{'+', 'age', 'invalid'}},
            {{'+', 'age', 1}},
            {{'+', 'age', 1}},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}})

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- upsert_many again
    -- fails for both: s1-master s2-master
    -- one error on each storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {10, box.NULL, 'Sergey', 25},
            {9, box.NULL, 'Anna', 30},
            {2, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 'Inga'},},
            {{'+', 'age', 1}},
            {{'+', 'age', 'invalid'}},
            {{'+', 'age', 'invalid'}},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- upsert_many again
    -- fails for both: s1-master s2-master
    -- two errors on each storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {1, box.NULL, 'Olga', 27},
            {71, box.NULL, 'Sergey', 25},
            {5, box.NULL, 'Anna', 30},
            {2, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
        },
        {
            {{'+', 'age', 1}},
            {{'+', 'age', 'invalid'}},
            {{'+', 'age', 'invalid'}},
            {{'+', 'age', 'invalid'}},
            {{'+', 'age', 'invalid'}},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 4)

    table.sort(errs, function(err1, err2) return err1.tuple[1] < err2.tuple[1] end)

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[1].tuple, {2, 401, 'Alex', 34})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[2].tuple, {3, 2804, 'Anastasia', 22})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[3].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[3].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[3].tuple, {5, 1172, "Anna", 30})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[4].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[4].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[4].tuple, {71, 1802, "Sergey", 25})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})
end

pgroup.test_upsert_many_rollback_and_stop_on_error = function(g)
    -- upsert_object_many
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18},
            {71, box.NULL, 'Oleg', 32}
        },
        {
            {{'+', 'age', 1}},
            {{'+', 'age', 1}},
            {{'+', 'age', 1}},
        },
        {
            stop_on_error = true,
            rollback_on_error  = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}, {}})

    -- get
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})

    -- upsert_many again
    -- stop_on_error = true, rollback_on_error = true
    -- one error on one storage with rollback, inserts stop by error on this storage
    -- inserts before error are rollbacked
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {22, box.NULL, 'Alex', 34},
            {92, box.NULL, 'Artur', 29},
            {3, box.NULL, 'Anastasia', 22},
            {5, box.NULL, 'Sergey', 25},
            {9, box.NULL, 'Anna', 30},
        },
        {
            {{'+', 'age', 1}},
            {{'+', 'age', 1}},
            {{'+', 'age', 'invalid'}},
            {{'+', 'age', 1}},
            {{'+', 'age', 1}},
        },
        {
            stop_on_error = true,
            rollback_on_error  = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[1].tuple, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{}, {}})

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)
end

pgroup.test_replace_object_many_get = function(g)
    -- bad format
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 1, name = 'Fedor', login = 'fedordostoevsky'},
            {id = 2, name = 'Anna'},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Field \"login\" isn\'t nullable')
    t.assert_equals(errs[1].tuple, {id = 2, name = 'Anna'})

    -- bad format
    -- two errors, default: stop_on_error == false
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 1, name = 'Fedor'},
            {id = 2, name = 'Anna'},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.tuple.id < err2.tuple.id end)

    t.assert_str_contains(errs[1].err, 'Field \"login\" isn\'t nullable')
    t.assert_equals(errs[1].tuple, {id = 1, name = 'Fedor'})

    t.assert_str_contains(errs[2].err, 'Field \"login\" isn\'t nullable')
    t.assert_equals(errs[2].tuple, {id = 2, name = 'Anna'})

    -- replace_object_many
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 1, name = 'Fedor', login = 'fedordostoevsky'},
            {id = 2, name = 'Anna', login = 'annaKar'},
            {id = 3, name = 'Daria', login = 'DMongen'}
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 1, name = 'Fedor', login = 'fedordostoevsky', bucket_id = 477},
        {id = 2, name = 'Anna', login = 'annaKar', bucket_id = 401},
        {id = 3, name = 'Daria', login = 'DMongen', bucket_id = 2804},
    })

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 'fedordostoevsky'})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 'annaKar'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 'DMongen'})

    -- replace_object_many again
    -- default: stop_on_error = false, rollback_on_error = false
    -- one error on one storage without rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 3, name = 'Alex', login = 'pushkinnn'},
            {id = 5, name = 'Sergey', login = 'Prof1234'},
            {id = 22, name = 'Anastasia', login = 'DMongen'},
            {id = 9, name = 'Anna', login = 'anna_boleyn'},
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {
        {id = 3, name = 'Alex', login = 'pushkinnn', bucket_id = 2804},
        {id = 5, name = 'Sergey', login = 'Prof1234', bucket_id = 1172},
        {id = 9, name = 'Anna', login = 'anna_boleyn', bucket_id = 1644},
        {id = 22, name = 'Anastasia', login = 'DMongen', bucket_id = 655},
    })

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(22)
    t.assert_equals(result, {22, 655, 'Anastasia', 'DMongen'})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 'Prof1234'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(3)
    t.assert_equals(result, {3, 2804, 'Alex', 'pushkinnn'})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(9)
    t.assert_equals(result, {9, 1644, 'Anna', 'anna_boleyn'})
end

pgroup.test_batch_insert_partial_result = function(g)
    -- bad fields format
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_many', {
        'customers',
        {
            {15, box.NULL, 'Fedor', 59},
            {25, box.NULL, 'Anna', 23},
        },
        {fields = {'id', 'invalid'}},
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space format doesn\'t contain field named "invalid"')

    -- batch_insert
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_many', {
        'customers',
        {
            {1, box.NULL, 'Fedor', 59},
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18}
        },
        {fields = {'id', 'name'}},
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {{id = 1, name = 'Fedor'}, {id = 2, name = 'Anna'}, {id = 3, name = 'Daria'}})
end

pgroup.test_batch_insert_object_partial_result = function(g)
    -- bad fields format
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 15, name = 'Fedor', age = 59},
            {id = 25, name = 'Anna', age = 23},
        },
        {fields = {'id', 'invalid'}},
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space format doesn\'t contain field named "invalid"')

    -- batch_insert_object
    local result, errs = g.cluster.main_server.net_box:call('crud.insert_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
        },
        {fields = {'id', 'name'}},
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.id < obj2.id end)
    t.assert_equals(objects, {{id = 1, name = 'Fedor'}, {id = 2, name = 'Anna'}, {id = 3, name = 'Daria'}})
end

pgroup.test_upsert_many_partial_result = function(g)
    -- bad fields format
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {15, box.NULL, 'Fedor', 59},
            {25, box.NULL, 'Anna', 23},
        },
        {
            {{'+', 'age', 1}, {'=', 'name', 'Peter'},},
            {{'+', 'age', 2}},
        },
        {fields = {'id', 'invalid'}},
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space format doesn\'t contain field named "invalid"')

    -- upsert_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {1, box.NULL, 'Fedor', 59},
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18}
        },
        {
            {{'+', 'age', 1}, {'=', 'name', 'Peter'},},
            {{'+', 'age', 2}},
            {{'+', 'age', 2}},
        },
        {fields = {'id', 'name'}},
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    })
    t.assert_equals(result.rows, {{}, {}, {}})

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})
end

pgroup.test_upsert_object_many_partial_result = function(g)
    -- bad fields format
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 15, name = 'Fedor', age = 59},
            {id = 25, name = 'Anna', age = 23},
        },
        {
            {{'+', 'age', 1}, {'=', 'name', 'Peter'},},
            {{'+', 'age', 2}},
        },
        {fields = {'id', 'invalid'}},
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space format doesn\'t contain field named "invalid"')

    -- upsert_object_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
        },
        {
            {{'+', 'age', 1}, {'=', 'name', 'Peter'}},
            {{'+', 'age', 2}},
            {{'+', 'age', 2}},
        },
        {fields = {'id', 'name'}},
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    })
    t.assert_equals(result.rows, {{}, {}, {}})

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})
end

pgroup.test_opts_not_damaged = function(g)
    -- batch insert
    local batch_insert_opts = {timeout = 1, fields = {'name', 'age'}}
    local new_batch_insert_opts, err = g.cluster.main_server:eval([[
        local crud = require('crud')

        local batch_insert_opts = ...

        local _, err = crud.insert_many('customers', {
            {1, box.NULL, 'Alex', 59}
        }, batch_insert_opts)

        return batch_insert_opts, err
    ]], {batch_insert_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_batch_insert_opts, batch_insert_opts)

    -- batch insert_object
    local batch_insert_opts = {timeout = 1, fields = {'name', 'age'}}
    local new_batch_insert_opts, err = g.cluster.main_server:eval([[
        local crud = require('crud')

        local batch_insert_opts = ...

        local _, err = crud.insert_object_many('customers', {
            {id = 2, name = 'Fedor', age = 59}
        }, batch_insert_opts)

        return batch_insert_opts, err
    ]], {batch_insert_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_batch_insert_opts, batch_insert_opts)

    -- batch upsert
    local upsert_many_opts = {timeout = 1, fields = {'name', 'age'}}
    local new_upsert_many_opts, err = g.cluster.main_server:eval([[
        local crud = require('crud')

        local upsert_many_opts = ...

        local _, err = crud.upsert_many('customers', {
            {1, box.NULL, 'Alex', 59}
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 'Leo Tolstoy'},}
        }, upsert_many_opts)

        return upsert_many_opts, err
    ]], {upsert_many_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_upsert_many_opts, upsert_many_opts)

    -- batch upsert_object
    local upsert_many_opts = {timeout = 1, fields = {'name', 'age'}}
    local new_upsert_many_opts, err = g.cluster.main_server:eval([[
        local crud = require('crud')

        local upsert_many_opts = ...

        local _, err = crud.upsert_object_many('customers', {
            {id = 2, name = 'Fedor', age = 59}
        },
        {
            {{'+', 'age', 25}, {'=', 'name', 'Leo Tolstoy'},}
        }, upsert_many_opts)

        return upsert_many_opts, err
    ]], {upsert_many_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_upsert_many_opts, upsert_many_opts)
end
