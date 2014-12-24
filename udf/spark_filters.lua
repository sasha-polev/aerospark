

function filter_by_column(stream, bins, name, val)
  local function split_line(line)
    values = {}
    for token in string.gmatch(line, "([^,]+)") do
      table.insert(values, token)
    end
    return values
  end

  bin_names = split_line(bins)

  local function filter_column1(record)
    return record[name] == val
  end

  local function map_profile(record)
    local out = map()
    for i=1, #bin_names do
      out[i] = record[bin_names[i]]
    end
    return out
  end

  return stream : filter(filter_column1) : map(map_profile)
end

function filter_in(stream, bins, name, list)
  local function split_line(line)
    values = {}
    for token in string.gmatch(line, "([^,]+)") do
      table.insert(values, token)
    end
    return values
  end

  bin_names = split_line(bins)

  filter_values = split_line(list)

  local function filter_columns(record)
    for i=1, #filter_values do
      if filter_values[i] == record[name] then
        return true
      end
    end
    return false
  end

  local function map_profile(record)
    local out = map()
    for i=1, #bin_names do
      out[i] = record[bin_names[i]]
    end
    return out
  end

  return stream : filter(filter_columns) : map(map_profile)
end


