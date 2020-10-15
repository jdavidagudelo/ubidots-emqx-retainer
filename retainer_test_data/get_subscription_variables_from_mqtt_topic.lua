-- luacheck: globals KEYS redis
local function validate_pattern(topic, pattern)
    local result = {}
    for w in string.gmatch(topic, pattern) do
        table.insert(result, w)
    end
    if #result == 1 then
        if result[1] == topic then
            return true
        end
    end
    return false
end

local function get_topics_mqtt(device_label, variable_label, topic_kind)
    local topic = '/v1.6/devices/' .. device_label .. '/' .. variable_label
    local result = {}
    if topic_kind == 'last_value' then
        topic = topic .. '/lv'
        table.insert(result, 'last_value')
        table.insert(result, topic)
    elseif topic_kind == 'value' then
        table.insert(result, 'value')
        table.insert(result, topic)
    elseif topic_kind == 'both' then
        local topic_lv = topic .. '/lv'
        table.insert(result, 'last_value')
        table.insert(result, topic_lv)
        table.insert(result, 'value')
        table.insert(result, topic)
    end
    return result
end

local function validate_patterns(topic, patterns)
    local result = false
    for i = 1, #patterns do
        local pattern = patterns[i]
        result = result or validate_pattern(topic, pattern)
    end
    return result
end

local function get_mqtt_topic_kind(topic)
    local regex_last_value = "/v1.6/users/[^/]+/devices/[a-zA-Z0-9_:.-]+/[a-zA-Z0-9_:.-]+/lv$"
    local regex_last_value_single_wild_card = "/v1.6/users/[^/]+/devices/[a-zA-Z0-9_:.-]+/[a-zA-Z0-9_:.-]+/%+$"
    local regex_last_value_multiple_wild_card = "/v1.6/users/[^/]+/devices/[a-zA-Z0-9_:.-]+/[a-zA-Z0-9_:.-]+/#$"
    local regex_last_value_variable_wild_cards = "/v1.6/users/[^/]+/devices/[a-zA-Z0-9_:.-]+/%+/lv$"
    local regex_last_value_variable_wild_cards_single = "/v1.6/users/[^/]+/devices/[a-zA-Z0-9_:.-]+/%+/%+$"
    local regex_last_value_variable_wild_cards_multiple = "/v1.6/users/[^/]+/devices/[a-zA-Z0-9_:.-]+/%+/#$"
    local regex_last_value_variable_device_wild_cards = "/v1.6/users/[^/]+/devices/%+/%+/lv$"
    local regex_last_value_variable_device_wild_cards_single = "/v1.6/users/[^/]+/devices/%+/%+/%+$"
    local regex_last_value_variable_device_wild_cards_multiple = "/v1.6/users/[^/]+/devices/%+/%+/#$"
    local regex_last_value_device_not_variable_wild_cards = "/v1.6/users/[^/]+/devices/%+/[a-zA-Z0-9_:.-]+/lv$"
    local regex_last_value_device_not_variable_wild_cards_single = "/v1.6/users/[^/]+/devices/%+/[a-zA-Z0-9_:.-]+/%+$"
    local regex_last_value_device_not_variable_wild_cards_multiple = "/v1.6/users/[^/]+/devices/%+/[a-zA-Z0-9_:.-]+/#$"

    local regex_value = "/v1.6/users/[^/]+/devices/[a-zA-Z0-9_:.-]+/[a-zA-Z0-9_:.-]+$"
    local regex_value_variable_wild_cards = "/v1.6/users/[^/]+/devices/[a-zA-Z0-9_:.-]+/%+$"
    local regex_value_device_variable_wild_cards = "/v1.6/users/[^/]+/devices/%+/%+$"
    local regex_value_device_not_variable_wild_cards = "/v1.6/users/[^/]+/devices/%+/[a-zA-Z0-9_:.-]+$"

    local regex_value_device_wild_cards = "/v1.6/users/[^/]+/devices/[a-zA-Z0-9_:.-]+/#$"
    local regex_value_device_two_wild_cards = "/v1.6/users/[^/]+/devices/%+/#$"
    local regex_value_user_wild_cards = "/v1.6/users/[^/]+/devices/#$"

    local last_value_patterns = {
        regex_last_value, regex_last_value_variable_wild_cards, regex_last_value_variable_device_wild_cards,
        regex_last_value_device_not_variable_wild_cards, regex_value_device_wild_cards,
        regex_value_device_two_wild_cards, regex_value_user_wild_cards,
        regex_last_value_single_wild_card, regex_last_value_multiple_wild_card,
        regex_last_value_variable_wild_cards_single, regex_last_value_variable_wild_cards_multiple,
        regex_last_value_variable_device_wild_cards_single, regex_last_value_variable_device_wild_cards_multiple,
        regex_last_value_device_not_variable_wild_cards_single, regex_last_value_device_not_variable_wild_cards_multiple
    }

    local value_patterns = {regex_value, regex_value_variable_wild_cards, regex_value_device_variable_wild_cards,
                            regex_value_device_not_variable_wild_cards, regex_value_device_wild_cards,
                            regex_value_device_two_wild_cards, regex_value_user_wild_cards}
    local is_last_value = validate_patterns(topic, last_value_patterns)
    local is_value = validate_patterns(topic, value_patterns)
    if is_last_value and is_value then
        return 'both'
    elseif is_last_value then
        return 'last_value'
    elseif is_value then
        return 'value'
    else
        return 'none'
    end
end


local function split(str, pat)
    local result = {}
    for w in string.gmatch(str, pat) do
        table.insert(result, w)
    end
    return result
end

local function decode_topic(topic)
    local topic_kind = get_mqtt_topic_kind(topic)
    if topic_kind == 'none' then
        local result = {}
        table.insert(result, '')
        table.insert(result, '')
        table.insert(result, '')
        table.insert(result, '')
        return result
    end
    local result = {}
    local split_data = split(topic, '/([^/]+)')
    local wild_card = {}
    wild_card['#'] = true
    wild_card['+'] = true
    local count = #split_data
    if count >= 3 then
        local current_token = split_data[3]
        table.insert(result, current_token)
        if count >= 5 then
            local current_device_label = split_data[5]
            if wild_card[current_device_label] then
                table.insert(result, '*')
            else
                table.insert(result, current_device_label)
            end
            if count >= 6 then
                local current_variable_label = split_data[6]
                if wild_card[current_variable_label] then
                    table.insert(result, '*')
                else
                    table.insert(result, current_variable_label)
                end
            else
                table.insert(result, '*')
            end
        end
    end
    table.insert(result, topic_kind)
    return result
end

local function get_table_from_hash_set(hash_set_key, device_labels, variable_label, topic_kind)
    local bulk_data = redis.call('HGETALL', hash_set_key)
    local result = {}
    local next_key
    local count = #bulk_data
    for i = 1, count do
        local v = bulk_data[i]
        if i % 2 == 1 then
            next_key = v
        else
            local split_data = split(next_key, "/([a-zA-Z0-9.:_-]+)")
            if #split_data == 2 then
                local current_device_label = split_data[1]
                local current_variable_label = split_data[2]
                local valid_variable_label = variable_label == '*' or variable_label == current_variable_label
                if device_labels[current_device_label] and valid_variable_label then
                    local topics = get_topics_mqtt(current_device_label, current_variable_label, topic_kind)
                    for k = 1, #topics, 2 do
                        table.insert(result, topics[k])
                        table.insert(result, topics[k + 1])
                        table.insert(result, v)
                    end
                end
            end
        end
    end
    return result
end

local function get_variables_device(device_labels, owner_id, variable_label, topic_kind)
    local reactor_variables_key = 'reactor_variables/' .. owner_id
    return get_table_from_hash_set(reactor_variables_key, device_labels, variable_label, topic_kind)
end

local function get_device_labels(device_ids)
    local result = {}
    for i = 1, #device_ids do
        local device_id = device_ids[i]
        local device_hash_set_key = 'reactor_device_data/' .. device_id
        result[redis.call('HGET', device_hash_set_key, '/device_label')] = true
    end
    return result
end

local function can_view_value_device_function(permissions_type, owner_id, device_label, token)
    local can_view_value_device = false
    if permissions_type == 'all' then
        can_view_value_device = true
    end
    local device_id = redis.call('HGET', 'reactor_variables/' .. owner_id, '/' .. device_label)
    if type(device_id) == 'string' then
        if permissions_type == 'device' and type(token) == 'string' then
            can_view_value_device = redis.call(
                'SISMEMBER', 'reactor_devices_with_permissions/view_value/' .. token, device_id
            )
        end
    end
    return can_view_value_device
end

local topic = KEYS[1]
local decoded_topic = decode_topic(topic)
local token = decoded_topic[1]
local device_label = decoded_topic[2]
local variable_label = decoded_topic[3]
local topic_kind = decoded_topic[4]

if type(token)  == 'string' then
    local token_hash_set_key = 'reactor_tokens/' .. token
    local owner_id = redis.call('HGET', token_hash_set_key, '/owner_id')
    local permissions_type = redis.call('HGET', token_hash_set_key, '/permissions_type')
    if type(owner_id) == 'string' then
        if device_label ~= "*" and variable_label ~= "*" then
            local can_view_value_device = can_view_value_device_function(
                permissions_type, owner_id, device_label, token
            )
            if can_view_value_device then
                local result = {}
                local variable_id = redis.call(
                    'HGET', 'reactor_variables/' .. owner_id, '/' .. device_label .. '/' .. variable_label
                )
                local topics = get_topics_mqtt(device_label, variable_label, topic_kind)
                for k = 1, #topics, 2 do
                    table.insert(result, topics[k])
                    table.insert(result, topics[k + 1])
                    table.insert(result, variable_id)
                end
                return result
            end
        elseif device_label ~= "*" then
            local can_view_value_device = can_view_value_device_function(
                permissions_type, owner_id, device_label, token
            )
            if can_view_value_device then
                local device_labels = {}
                device_labels[device_label] = true
                local result =  get_variables_device(device_labels, owner_id, variable_label, topic_kind)
                return result
            end
        else
            local device_ids = redis.call('SMEMBERS', 'reactor_devices_with_permissions/view_value/' .. token)
            local device_labels = get_device_labels(device_ids)
            local result = get_variables_device(device_labels, owner_id, variable_label, topic_kind)
            return result
        end
    end
end

return {}