local prefix = ARGV[1]
local limit = tonumber(ARGV[2])
local worker_id = ARGV[3]
local lease_timeout = tonumber(ARGV[4])
local modes = cjson.decode(ARGV[5])

local MAX_I64 = '9223372036854775807'
local MAX_SEQUENCE = '99999999999999999999999999999999'
local MAX_RECOVERY = 128
local MAX_RECOVERY_PER_MODE = math.floor(MAX_RECOVERY / 2)
local MAX_PROMOTION = 128
local MAX_INSPECTION = 128
local MAX_SELECTION = 128
local MAX_PROCESSING_INSPECTION = math.floor(MAX_INSPECTION / 2)

local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
local now_text = time[1] .. string.format('%03d', math.floor(tonumber(time[2]) / 1000))
local expired_before = now - lease_timeout

local function storage_type(key)
    return redis.call('TYPE', key).ok
end

local function accepts_type(key, expected)
    local actual = storage_type(key)
    return actual == 'none' or actual == expected
end

local function indexes_are_valid()
    if not accepts_type(KEYS[1], 'hash')
        or not accepts_type(KEYS[2], 'hash') then
        return false
    end
    for _, mode in ipairs({'http', 'browser'}) do
        if not accepts_type(prefix .. 'queue:' .. mode .. ':ready', 'zset')
            or not accepts_type(prefix .. 'queue:' .. mode .. ':delayed', 'zset')
            or not accepts_type(prefix .. 'processing:' .. mode, 'zset') then
            return false
        end
    end
    return true
end

if not indexes_are_valid() then return redis.error_reply('CORRUPT_INDEX_TYPE') end

local function pad(value, width)
    value = tostring(value)
    return string.rep('0', width - string.len(value)) .. value
end

local function request_key(token)
    return prefix .. 'request:' .. token
end

local function token_from_member(member)
    if type(member) ~= 'string' then return nil end
    return string.match(member, '([^|]+)$')
end

local function member_token(kind, member)
    if type(member) ~= 'string' then return nil end
    if kind == 'ready' then
        local sequence, token = string.match(member, '^(%d+)|([^|]+)$')
        if sequence and #sequence == 32 then return token end
    elseif kind == 'delayed' then
        local due_time, sequence, token = string.match(member, '^(%d+)|(%d+)|([^|]+)$')
        if due_time and #due_time == 19 and sequence and #sequence == 32 then
            return token
        end
    end
    return nil
end

local function increment_decimal(value, max, width)
    if type(value) ~= 'string' or not string.match(value, '^%d+$') then return nil end
    value = string.gsub(value, '^0+', '')
    if value == '' then value = '0' end
    if string.len(value) > width or (string.len(value) == width and value >= max) then
        return nil
    end

    local digits = {string.byte(value, 1, -1)}
    local index = #digits
    while index > 0 and digits[index] == 57 do
        digits[index] = 48
        index = index - 1
    end
    if index == 0 then
        table.insert(digits, 1, 49)
    else
        digits[index] = digits[index] + 1
    end
    return string.char(unpack(digits))
end

local function plan_sequences(count)
    local current = redis.call('HGET', KEYS[1], 'enqueue_sequence') or '0'
    local sequences = {}
    for _ = 1, count do
        current = increment_decimal(current, MAX_SEQUENCE, 32)
        if not current then return nil end
        table.insert(sequences, string.rep('0', 32 - string.len(current)) .. current)
    end
    return sequences, current
end

local function next_version(value)
    return increment_decimal(value, MAX_I64, 19)
end

local function parse_mode(value)
    if value == 'http' or value == 'browser' then return value end
    return nil
end

local function parse_priority(value)
    if type(value) ~= 'string' or not string.match(value, '^%-?%d+$') then return nil end
    local parsed = tonumber(value)
    if not parsed or parsed < -2147483648 or parsed > 2147483647 then return nil end
    return parsed
end

local function parse_retry(value)
    if type(value) ~= 'string' or not string.match(value, '^%d+$') then return nil end
    local parsed = tonumber(value)
    if not parsed or parsed > 2147483647 then return nil end
    return parsed
end

local function remove_active(token, mode)
    if parse_mode(mode) then
        redis.call('ZREM', prefix .. 'processing:' .. mode, token)
        local other = mode == 'http' and 'browser' or 'http'
        redis.call('ZREM', prefix .. 'processing:' .. other, token)
    else
        redis.call('ZREM', prefix .. 'processing:http', token)
        redis.call('ZREM', prefix .. 'processing:browser', token)
    end
end

local function remove_stored_queue(key, token)
    local mode = parse_mode(redis.call('HGET', key, 'mode'))
    local queue_kind = redis.call('HGET', key, 'queue_kind')
    local member = redis.call('HGET', key, 'queue_member')
    local stored_token = member_token(queue_kind, member) or token_from_member(member)
    if (queue_kind == 'ready' or queue_kind == 'delayed')
        and member and member ~= '' and stored_token == token then
        if mode then
            redis.call('ZREM', prefix .. 'queue:' .. mode .. ':' .. queue_kind, member)
        else
            redis.call('ZREM', prefix .. 'queue:http:' .. queue_kind, member)
            redis.call('ZREM', prefix .. 'queue:browser:' .. queue_kind, member)
        end
    end
end

local function write_completion(key, token, version, worker, message)
    local completion = request_key(token) .. ':completion:' .. version
    local kind = storage_type(completion)
    if kind ~= 'none' and kind ~= 'hash' then redis.call('DEL', completion) end
    redis.call('HSET', completion,
        'task_id', redis.call('HGET', key, 'task_id') or '',
        'trace_id', redis.call('HGET', key, 'trace_id') or '',
        'node', redis.call('HGET', key, 'node') or '',
        'worker_id', worker or '',
        'state', 'failed',
        'error', message)
end

local function record_failed_worker(token, worker)
    if not worker or worker == '' then return end
    local failed_workers = request_key(token) .. ':failed_workers'
    local kind = storage_type(failed_workers)
    if kind ~= 'none' and kind ~= 'list' then redis.call('DEL', failed_workers) end
    if redis.call('LPOS', failed_workers, worker) == false then
        redis.call('RPUSH', failed_workers, worker)
    end
end

local function quarantine(key, token, message)
    if storage_type(key) ~= 'hash' then
        remove_active(token, nil)
        return
    end

    local version = redis.call('HGET', key, 'version')
    if type(version) ~= 'string' or not string.match(version, '^%d+$') then version = '0' end
    local worker = redis.call('HGET', key, 'leased_by') or ''
    local mode = redis.call('HGET', key, 'mode')
    remove_active(token, mode)
    remove_stored_queue(key, token)
    write_completion(key, token, version, worker, message)
    redis.call('HSET', key,
        'state', 'failed', 'leased_by', '', 'lease_time', '0', 'ack_version', '',
        'queue_kind', '', 'queue_member', '', 'updated_time', now_text)
end

-- A queue can contain an orphaned member after its Request moved elsewhere.
-- Preserve a known active or terminal Request, and preserve a pending Request
-- that still has another valid membership. Other recoverable records are
-- quarantined so malformed state cannot remain pending without an index.
local function discard_queue_member(mode, kind, member, message)
    redis.call('ZREM', prefix .. 'queue:' .. mode .. ':' .. kind, member)

    local token = token_from_member(member)
    if not token then return end

    local key = request_key(token)
    if storage_type(key) ~= 'hash' then
        remove_active(token, nil)
        return
    end

    local state = redis.call('HGET', key, 'state')
    if state == 'processing' or state == 'done' or state == 'failed' then return end

    -- A Request can have a legitimate membership elsewhere when this member is
    -- a duplicate dangling index. Preserve that Request and only discard this
    -- member. Any other pending mismatch is not recoverable from queue state.
    local stored_mode = parse_mode(redis.call('HGET', key, 'mode'))
    local stored_kind = redis.call('HGET', key, 'queue_kind')
    local stored_member = redis.call('HGET', key, 'queue_member')
    if state == 'pending'
        and stored_mode and (stored_kind == 'ready' or stored_kind == 'delayed')
        and stored_member and stored_member ~= ''
        and member_token(stored_kind, stored_member) == token
        and redis.call('ZSCORE', prefix .. 'queue:' .. stored_mode .. ':' .. stored_kind,
            stored_member) ~= false then
        return
    end

    quarantine(key, token, message)
end

local sequences = {}
local sequence_index = 1
local function enqueue(key, mode, priority, next_time, token)
    local sequence = sequences[sequence_index]
    sequence_index = sequence_index + 1
    if pad(next_time, 19) <= pad(now_text, 19) then
        local member = sequence .. '|' .. token
        redis.call('ZADD', prefix .. 'queue:' .. mode .. ':ready', -priority, member)
        redis.call('HSET', key, 'queue_kind', 'ready', 'queue_member', member)
    else
        local member = pad(next_time, 19) .. '|' .. sequence .. '|' .. token
        redis.call('ZADD', prefix .. 'queue:' .. mode .. ':delayed', 0, member)
        redis.call('HSET', key, 'queue_kind', 'delayed', 'queue_member', member)
    end
end

local actions = {}
local requeues = 0
local expired_tokens = {}

local function lease_millis(value)
    if type(value) ~= 'string' or not string.match(value, '^%d+$') then return nil end
    value = string.gsub(value, '^0+', '')
    if value == '' then value = '0' end
    if string.len(value) > 16 or (string.len(value) == 16 and value > '9007199254740991') then
        return nil
    end
    return tonumber(value)
end

local function collect_expired(mode)
    local processing = prefix .. 'processing:' .. mode
    local expired = redis.call('ZRANGEBYSCORE', processing, '-inf', expired_before,
        'LIMIT', 0, MAX_RECOVERY_PER_MODE)

    for _, token in ipairs(expired) do
        if not expired_tokens[token] then
            expired_tokens[token] = true
            local key = request_key(token)
            local action = {
                token = token,
                key = key,
                index_mode = mode,
                lease_score = tonumber(redis.call('ZSCORE', processing, token))
            }
            if storage_type(key) ~= 'hash' then
                action.kind = 'missing'
            else
                local state = redis.call('HGET', key, 'state')
                action.mode = redis.call('HGET', key, 'mode')
                if state ~= 'processing' then
                    if state == 'pending' or state == 'done' or state == 'failed' then
                        action.kind = 'stale'
                    else
                        action.kind = 'corrupt'
                    end
                else
                    action.version = redis.call('HGET', key, 'version')
                    action.mode = parse_mode(action.mode)
                    action.priority = parse_priority(redis.call('HGET', key, 'priority'))
                    action.retry_count = parse_retry(redis.call('HGET', key, 'retry_count'))
                    action.max_retry_count = parse_retry(redis.call('HGET', key, 'max_retry_count'))
                    action.worker = redis.call('HGET', key, 'leased_by') or ''
                    action.lease_time = lease_millis(redis.call('HGET', key, 'lease_time'))
                    action.acknowledged = redis.call('HGET', key, 'ack_version') == action.version

                    if type(action.version) ~= 'string' or not string.match(action.version, '^%d+$')
                        or not action.mode
                        or not action.priority
                        or not action.retry_count
                        or not action.max_retry_count
                        or action.max_retry_count <= 0
                        or action.retry_count >= action.max_retry_count
                        or not action.lease_time
                        or not action.lease_score then
                        action.kind = 'corrupt'
                    elseif action.mode ~= mode or action.lease_score ~= action.lease_time then
                        -- The Hash is authoritative. Repair an incorrectly indexed
                        -- active Request before considering lease expiry.
                        action.kind = 'repair'
                    elseif action.acknowledged then
                        action.retry = action.retry_count + 1
                        action.kind = action.retry < action.max_retry_count and 'requeue' or 'terminal'
                    else
                        action.retry = action.retry_count
                        action.kind = 'requeue'
                    end
                end
            end
            table.insert(actions, action)
        end
    end
end

for _, mode in ipairs({'http', 'browser'}) do
    collect_expired(mode)
end

-- Each mode contributes at most half of the bounded recovery batch, so equal
-- lease timestamps cannot starve either capability.
table.sort(actions, function(left, right)
    local left_score = left.lease_score or math.huge
    local right_score = right.lease_score or math.huge
    if left_score ~= right_score then return left_score < right_score end
    if left.index_mode ~= right.index_mode then return left.index_mode < right.index_mode end
    return left.token < right.token
end)
while #actions > MAX_RECOVERY do
    table.remove(actions)
end
for _, action in ipairs(actions) do
    if action.kind == 'requeue' then requeues = requeues + 1 end
end

local planned, final_sequence = plan_sequences(requeues)
if not planned then return redis.error_reply('SEQUENCE_OVERFLOW') end
sequences = planned
if requeues > 0 then redis.call('HSET', KEYS[1], 'enqueue_sequence', final_sequence) end

for _, action in ipairs(actions) do
    if action.kind == 'missing' then
        remove_active(action.token, nil)
    elseif action.kind == 'stale' then
        remove_active(action.token, action.mode)
    elseif action.kind == 'corrupt' then
        quarantine(action.key, action.token, 'stored Request has invalid lease state')
    elseif action.kind == 'repair' then
        remove_active(action.token, nil)
        redis.call('ZADD', prefix .. 'processing:' .. action.mode, action.lease_time, action.token)
    else
        remove_active(action.token, action.mode)
        if action.acknowledged then
            record_failed_worker(action.token, action.worker)
            write_completion(action.key, action.token, action.version, action.worker,
                'acknowledged lease expired')
        end
        if action.kind == 'requeue' then
            redis.call('HSET', action.key,
                'state', 'pending', 'retry_count', action.retry, 'next_time', '0',
                'leased_by', '', 'lease_time', '0', 'ack_version', '', 'updated_time', now_text)
            enqueue(action.key, action.mode, action.priority, 0, action.token)
        else
            redis.call('HSET', action.key,
                'state', 'failed', 'retry_count', action.retry, 'next_time', '0',
                'leased_by', '', 'lease_time', '0', 'ack_version', '',
                'queue_kind', '', 'queue_member', '', 'updated_time', now_text)
        end
    end
end

local function inspection_start(key, field)
    local total = redis.call('ZCARD', key)
    if total == 0 then
        redis.call('HDEL', KEYS[1], field)
        return nil
    end

    local stored = redis.call('HGET', KEYS[1], field)
    local offset = 0
    if type(stored) == 'string' and string.match(stored, '^%d+$') and #stored <= 10 then
        offset = tonumber(stored) or 0
    end
    if offset >= total then offset = 0 end
    return offset
end

local function inspection_end(key, field, offset, count)
    local total = redis.call('ZCARD', key)
    if total == 0 then
        redis.call('HDEL', KEYS[1], field)
    else
        redis.call('HSET', KEYS[1], field, (offset + count) % total)
    end
end

local function inspect_processing(index_mode, token, score)
    local key = request_key(token)
    if storage_type(key) ~= 'hash' then
        redis.call('ZREM', prefix .. 'processing:' .. index_mode, token)
        return
    end

    local state = redis.call('HGET', key, 'state')
    if state == 'pending' or state == 'done' or state == 'failed' then
        remove_active(token, redis.call('HGET', key, 'mode'))
        return
    end
    if state ~= 'processing' then
        quarantine(key, token, 'stored Request lease has an invalid state')
        return
    end

    local mode = parse_mode(redis.call('HGET', key, 'mode'))
    local worker = redis.call('HGET', key, 'leased_by')
    local version = redis.call('HGET', key, 'version')
    local lease_time = lease_millis(redis.call('HGET', key, 'lease_time'))
    local lease_score = tonumber(score)
    local version_valid = next_version(version) ~= nil or version == MAX_I64
    if not mode
        or type(worker) ~= 'string' or worker == ''
        or not version_valid
        or not lease_time
        or not lease_score
    then
        quarantine(key, token, 'stored Request lease does not match its fields')
    elseif mode ~= index_mode or lease_score ~= lease_time then
        -- The Request Hash is authoritative. Repair a stale or misplaced
        -- processing index without changing retry state.
        remove_active(token, nil)
        redis.call('ZADD', prefix .. 'processing:' .. mode, lease_time, token)
    end
end

local function inspect_processing_index(mode)
    local processing = prefix .. 'processing:' .. mode
    local field = 'processing_scan:' .. mode
    local offset = inspection_start(processing, field)
    if not offset then return end

    local values = redis.call('ZRANGE', processing,
        offset, offset + MAX_PROCESSING_INSPECTION - 1,
        'WITHSCORES')
    local count = #values / 2
    for index = 1, #values, 2 do
        inspect_processing(mode, values[index], values[index + 1])
    end
    inspection_end(processing, field, offset, count)
end

for _, mode in ipairs({'http', 'browser'}) do
    inspect_processing_index(mode)
end

local function delayed_request(mode, member)
    local due_time, sequence, token = string.match(member, '^(%d+)|(%d+)|([^|]+)$')
    if not due_time or #due_time ~= 19 or not sequence or #sequence ~= 32 then
        discard_queue_member(mode, 'delayed', member,
            'stored Request delayed queue has an invalid member')
        return nil
    end

    local key = request_key(token)
    if storage_type(key) ~= 'hash' then
        discard_queue_member(mode, 'delayed', member,
            'stored Request delayed queue has no Request record')
        return nil
    end

    if redis.call('HGET', key, 'state') ~= 'pending' then
        discard_queue_member(mode, 'delayed', member,
            'stored Request delayed queue has an invalid state')
        return nil
    end

    local stored_mode = parse_mode(redis.call('HGET', key, 'mode'))
    local priority = parse_priority(redis.call('HGET', key, 'priority'))
    local next_time = redis.call('HGET', key, 'next_time')
    local queue_matches = redis.call('HGET', key, 'queue_kind') == 'delayed'
        and redis.call('HGET', key, 'queue_member') == member
    if stored_mode ~= mode
        or not priority
        or not next_time
        or not string.match(next_time, '^%d+$')
        or #next_time > 19
        or pad(next_time, 19) ~= due_time
        or not queue_matches then
        discard_queue_member(mode, 'delayed', member,
            'stored Request delayed queue does not match its fields')
        return nil
    end

    return key, sequence, token, priority
end

local function inspect_delayed(mode)
    local delayed = prefix .. 'queue:' .. mode .. ':delayed'
    local field = 'delayed_scan:' .. mode
    local offset = inspection_start(delayed, field)
    if not offset then return end

    local values = redis.call('ZRANGE', delayed, offset, offset + MAX_INSPECTION - 1,
        'WITHSCORES')
    local count = #values / 2
    for index = 1, #values, 2 do
        if tonumber(values[index + 1]) ~= 0 then
            discard_queue_member(mode, 'delayed', values[index],
                'stored Request delayed queue has an invalid score')
        else
            delayed_request(mode, values[index])
        end
    end
    inspection_end(delayed, field, offset, count)
end

for _, mode in ipairs(modes) do
    inspect_delayed(mode)
end

for _, mode in ipairs(modes) do
    local delayed = prefix .. 'queue:' .. mode .. ':delayed'
    local due = redis.call('ZRANGEBYLEX', delayed, '-',
        '[' .. pad(now_text, 19) .. '|' .. string.char(255), 'LIMIT', 0, MAX_PROMOTION)
    for _, member in ipairs(due) do
        local key, sequence, token, priority = delayed_request(mode, member)
        if key then
            local ready_member = sequence .. '|' .. token
            redis.call('ZADD', prefix .. 'queue:' .. mode .. ':ready', -priority, ready_member)
            redis.call('ZREM', delayed, member)
            redis.call('HSET', key, 'queue_kind', 'ready', 'queue_member', ready_member)
        end
    end
end

local claimed = {}
local discarded = 0
while #claimed < limit do
    local selected_mode = nil
    local selected_member = nil
    local selected_score = nil
    for _, mode in ipairs(modes) do
        local value = redis.call('ZRANGE', prefix .. 'queue:' .. mode .. ':ready', 0, 0, 'WITHSCORES')
        if #value > 0 then
            local member = value[1]
            local score = tonumber(value[2])
            if not selected_member or score < selected_score
                or (score == selected_score and member < selected_member) then
                selected_mode = mode
                selected_member = member
                selected_score = score
            end
        end
    end
    if not selected_member then break end
    local claimed_before = #claimed

    local sequence, token = string.match(selected_member, '^(%d+)|([^|]+)$')
    if not sequence or #sequence ~= 32 then
        discard_queue_member(selected_mode, 'ready', selected_member,
            'stored Request ready queue has an invalid member')
    else
        local key = request_key(token)
        if storage_type(key) ~= 'hash' then
            redis.call('ZREM', prefix .. 'queue:' .. selected_mode .. ':ready', selected_member)
            remove_active(token, nil)
        else
            local state = redis.call('HGET', key, 'state')
            if state ~= 'pending' then
                discard_queue_member(selected_mode, 'ready', selected_member,
                    'stored Request ready queue has an invalid state')
            else
                local stored_mode = parse_mode(redis.call('HGET', key, 'mode'))
                local priority = parse_priority(redis.call('HGET', key, 'priority'))
                local version = redis.call('HGET', key, 'version')
                local retry_count = parse_retry(redis.call('HGET', key, 'retry_count'))
                local max_retry_count = parse_retry(redis.call('HGET', key, 'max_retry_count'))
                local next_time = redis.call('HGET', key, 'next_time')
                local queue_matches = redis.call('HGET', key, 'queue_kind') == 'ready'
                    and redis.call('HGET', key, 'queue_member') == selected_member
                local next = next_version(version)
                if stored_mode ~= selected_mode
                    or not priority
                    or not retry_count
                    or not max_retry_count
                    or max_retry_count <= 0
                    or retry_count >= max_retry_count
                    or not next_time
                    or not string.match(next_time, '^%d+$')
                    or #next_time > 19
                    or pad(next_time, 19) > pad(now_text, 19)
                    or not queue_matches then
                    discard_queue_member(selected_mode, 'ready', selected_member,
                        'stored Request ready queue does not match its fields')
                elseif version == MAX_I64 then
                    discard_queue_member(selected_mode, 'ready', selected_member,
                        'request version overflow while claiming')
                elseif not next then
                    discard_queue_member(selected_mode, 'ready', selected_member,
                        'stored Request has invalid version')
                else
                    redis.call('ZREM', prefix .. 'queue:' .. selected_mode .. ':ready',
                        selected_member)
                    redis.call('HSET', key,
                        'state', 'processing', 'version', next, 'leased_by', worker_id,
                        'lease_time', now_text, 'ack_version', '', 'queue_kind', '',
                        'queue_member', '', 'updated_time', now_text)
                    -- A Request must have one active mode projection even if an
                    -- earlier damaged index placed the token in the other mode.
                    remove_active(token, nil)
                    redis.call('ZADD', prefix .. 'processing:' .. stored_mode, now, token)

                    local failed_workers_key = request_key(token) .. ':failed_workers'
                    local failed_workers_type = storage_type(failed_workers_key)
                    if failed_workers_type ~= 'none' and failed_workers_type ~= 'list' then
                        redis.call('DEL', failed_workers_key)
                    end
                    local failed_workers = redis.call('LRANGE', failed_workers_key, 0, -1)
                    if #failed_workers == 0 then failed_workers = cjson.empty_array end

                    local trace_id = redis.call('HGET', key, 'trace_id') or ''
                    local trace = cjson.null
                    if trace_id ~= '' then
                        local stored = redis.call('HGET', KEYS[2], trace_id)
                        if stored then trace = stored end
                    end
                    table.insert(claimed, cjson.encode({
                        token = token,
                        id = redis.call('HGET', key, 'id') or '',
                        task_id = redis.call('HGET', key, 'task_id') or '',
                        trace_id = trace_id,
                        node = redis.call('HGET', key, 'node') or '',
                        mode = stored_mode,
                        priority = priority,
                        next_time = redis.call('HGET', key, 'next_time') or '0',
                        version = next,
                        retry_count = retry_count,
                        max_retry_count = max_retry_count,
                        leased_by = worker_id,
                        lease_time = now_text,
                        snapshot = redis.call('HGET', key, 'snapshot') or '',
                        digest = redis.call('HGET', key, 'digest') or '',
                        trace = trace,
                        failed_workers = failed_workers
                    }))
                end
            end
        end
    end

    if #claimed == claimed_before then
        discarded = discarded + 1
        if discarded >= MAX_SELECTION then break end
    end
end

return claimed
