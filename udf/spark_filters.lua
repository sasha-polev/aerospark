-- Function accepts multiple  filters, in format {filterType, filterColumn, values (depending on type)}
-- filterType: 0 or nil - none, 1 - equalsString, 2 - equalsLong, 3 - range, 4 - inset
function multifilter(stream, bins, ...)

    local function tail2(list)
        local out2 = {}
        for i = 3, #list do
            table.insert(out2, list[i])
        end
        return out2
    end

    local function f_in(name, filter_values)
        return function(record)
            for _, v in ipairs(filter_values) do
                if v == record[name] then
                    return true
                end
            end
            return false
        end
    end

    local function f_range(name, filter_values)
        return function(record)
            return record[name] >= filter_values[1] and record[name] <= filter_values[2]
        end
    end

    local filters = {}

    --if not arg == nil then
    for i, v in ipairs({...}) do
        --warn("i: %s", i)
        --warn("v: %s", v)
        -- here we iterate function parameters and create table of filter functions
        local t = v[1] -- filter type
        local n = v[2] -- bin name to filter on
        if t == 1 or t == 2 or t == 4 then -- equal or inset are translated to same inset filter
        table.insert(filters, f_in(n, tail2(v)))
        elseif t == 3 then
            table.insert(filters, f_range(n, tail2(v)))
        end
    end
    --end

    local function filter_columns(record)
        for _, f in ipairs(filters) do
            if not f(record) then
                return false
            end
        end
        return true
    end

    local function map_profile(record)
        local out = map()
        for i = 1, #bins do
            out[i] = record[bins[i]]
        end
        return out
    end

    return stream:filter(filter_columns):map(map_profile)
end