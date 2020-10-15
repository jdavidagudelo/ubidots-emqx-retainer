-- luacheck: globals KEYS redis cjson
local result = {}
for i = 1, #KEYS, 3 do
    local value_kind = KEYS[i]
    local topic = KEYS[i + 1]
    local variable_id = KEYS[i + 2]
    local variable_value_key = 'last_value_variables_json:' .. variable_id
    local variable_string_value_key = 'last_value_variables_string:' .. variable_id
    local value_data = redis.call("GET", variable_value_key)
    if type(value_data) == 'string' then
        if value_kind == 'value' then
            table.insert(result, topic)
            table.insert(result,  value_data)
        elseif value_kind == 'last_value' then
            local value_json = cjson.decode(value_data)
            local value = value_json['value']
            local string_value = redis.call("GET", variable_string_value_key)
            if type(string_value) == 'string' then
                table.insert(result, topic)
                table.insert(result, string_value)
            elseif value ~= nil then
                table.insert(result, topic)
                table.insert(result, value)
            end
        end
    end
end
return result