local urlparse = require("socket.url")
local http = require("socket.http")
local cjson = require("cjson")
local utf8 = require("utf8")
local openssl_digest = require("openssl.digest")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false
local status_code = 0

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false
local retry_after = nil
local context = {}

local date_pattern = "([0-9][0-9][0-9][0-9]%-[0-9][0-9]%-[0-9][0-9])"
local item_patterns = {
  ["^https?://data%.world/api/search.*[%?&]filters=created%%3[Dd]" .. date_pattern]="search",
  ["^https?://data%.world/api/search.*[%?&]filters=created=" .. date_pattern]="search",
  ["^https?://data%.world/api/search.*[%?&]filters=updated%%3[Dd]" .. date_pattern]="search",
  ["^https?://data%.world/api/search.*[%?&]filters=updated=" .. date_pattern]="search",
  ["^https?://data%.world/([0-9a-z%-]+/[0-9a-z%._%-]+)/?$"]="project",
  ["^https?://(media%.data%.world/.+)$"]="asset",
  ["^https?://(mediauploads[^%.]*%.data%.world/.+)$"]="asset",
  ["^https?://(cdn%.filepicker%.io/.+)$"]="asset",
  ["^https?://(cdn%.filestackcontent%.com/.+)$"]="asset",
  ["^https?://(data%.world/api/chart/export/.+)$"]="asset"
}

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  io.stdout:flush()
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file, "rb"))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if item ~= item_name and not target[item] then
    target[item] = true
    return true
  end
  return false
end

url_encode = function(value)
  return string.gsub(value, "([^0-9a-zA-Z%-_%.~])", function(c)
    return string.format("%%%02X", string.byte(c))
  end)
end

percent_encode_url = function(newurl)
  return string.gsub(newurl, "(.)", function(c)
    local b = string.byte(c)
    if b < 32 or b > 126 then
      return string.format("%%%02X", b)
    end
    return c
  end)
end

find_item = function(url)
  for pattern, item_type_ in pairs(item_patterns) do
    local value = string.match(url, pattern)
    if value
      and not (item_type_ == "project"
        and string.match(value, "^api/")) then
      return {
        ["value"]=value,
        ["type"]=item_type_
      }
    end
  end
end

set_item = function(url)
  if ids[string.lower(url)] then
    return nil
  end
  local found = find_item(url)
  if found then
    local new_item_type = found["type"]
    local new_item_value = found["value"]
    local new_item_name = new_item_type .. ":" .. new_item_value
    if new_item_name ~= item_name then
      ids = {}
      context = {}
      item_value = new_item_value
      item_type = new_item_type
      ids[string.lower(item_value)] = true
      ids[string.lower(url)] = true
      if item_type == "project" then
        context["files"] = {}
        context["todo"] = {}
        context["owner"], context["project"] = string.match(item_value, "^([^/]+)/(.+)$")
        ids[string.lower(context["owner"])] = true
        ids[string.lower(context["project"])] = true
      end
      abortgrab = false
      tries = 0
      retry_url = false
      item_name = new_item_name
      print("Archiving item " .. item_name)
    end
  end
end

allowed = function(url, parenturl)
  local lower = string.lower(url)
  if ids[lower] then
    return true
  end

  local found = find_item(url)
  if found then
    local new_item = found["type"] .. ":" .. found["value"]
    if new_item ~= item_name then
      if (found["type"] == "project" or found["type"] == "asset")
        and not string.match(url, "%.%.%.$") then
        discover_item(discovered_items, new_item)
      end
      return false
    end
    return true
  end

  if not string.match(lower, "^https?://data%.world[/%?:]")
    and not string.match(lower, "^https?://[^/]+%.data%.world[/%?:]") then
    if not string.match(lower, "^https?://view%.dwcontent%.com[/%?:]") then
      discover_item(discovered_outlinks, string.match(percent_encode_url(url), "^([^%s]+)"))
    end
    return false
  end

  if string.match(lower, "^https?://[^/]+%.linked%.data%.world[/%?:]")
    or string.match(lower, "^https?://download%.data%.world/query_result_download/")
    or (
      (
        string.match(lower, "^https?://download%.data%.world/download/")
        or string.match(lower, "^https?://download%.data%.world/file_download/")
      )
      and not string.match(lower, "[%?&]dwr=")
    )
    or (
      string.match(lower, "^https?://download%.data%.world/prebuilt_view/")
      and not string.match(lower, "[%?&]authentication=")
    )
    or string.match(lower, "^https?://query%.data%.world/sparql/") then
    return false
  end

  for value in string.gmatch(string.lower(urlparse.unescape(url)), "([0-9a-z_%.%-]+)") do
    if ids[value] then
      return true
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function(s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local data = nil

  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function check(newurl, headers, body_data, method)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://") then
      return nil
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "[%s\\]") then
      return nil
    end
    local origurl = url
    if string.len(url) == 0
      or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = url
    while string.match(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    local key = (method or "GET") .. "\0" .. url_ .. "\0" .. tostring(body_data)
    if not processed(key)
      and (body_data or not processed(url_))
      and allowed(url_, origurl) then
      local url_data = {
        url=url_,
        headers=headers or {}
      }
      if body_data then
        url_data["body_data"] = body_data
        url_data["method"] = method or "POST"
      end
      table.insert(urls, url_data)
      addedtolist[key] = true
      if not body_data then
        addedtolist[url_] = true
        addedtolist[url] = true
      end
      return true
    end
  end

  local function set_param(newurl, param, value)
    if string.match(newurl, "[%?&]" .. param .. "=") then
      return string.gsub(newurl, "([%?&]" .. param .. "=)[^&]*", "%1" .. value, 1)
    end
    if string.match(newurl, "%?") then
      return newurl .. "&" .. param .. "=" .. value
    end
    return newurl .. "?" .. param .. "=" .. value
  end

  local function paginate_search(json)
    local records = json["records"]
    local count = tonumber(json["count"])
    local current_from = tonumber(string.match(url, "[%?&]from=([0-9]+)")) or 0
    if not count or type(records) ~= "table" then
      abort_item()
    elseif count > 10000 then
      local search_type = string.match(url, "[%?&]type=([^&]+)")
      local filter = string.match(url, "[%?&]filters=([^&]+)")
      local field, value
      if filter then
        field, value = string.match(urlparse.unescape(filter), "^([^=]+)=(.+)$")
      end
      local suffix = nil
      local limit = nil
      if value and string.match(value, "^[0-9]+%-[0-9]+%-[0-9]+$") then
        suffix = "T%02d"
        limit = 23
      elseif value and (
          string.match(value, "T[0-9][0-9]$")
          or string.match(value, "T[0-9][0-9]:[0-9][0-9]$")
        ) then
        suffix = ":%02d"
        limit = 59
      end
      if not search_type or not field or not limit then
        abort_item()
      else
        for part = 0, limit do
          check("https://data.world/api/search?type=" .. search_type .. "&size=100&from=0&filters=" .. url_encode(field .. "=" .. value .. string.format(suffix, part)))
        end
      end
    elseif #records == 0 and current_from < count then
      abort_item()
    elseif current_from + #records < count then
      check(set_param(url, "from", tostring(current_from + #records)))
    end
    return records
  end

  local function scan_value(value)
    if type(value) == "table" then
      for _, child in pairs(value) do
        scan_value(child)
      end
    elseif type(value) == "string" then
      check(value)
    end
  end

  local function walk_version(value, versionid)
    if value["path"] and value["hash"] then
      local todo = "https://download.data.world/prebuilt_view/" .. item_value .. "/" .. versionid .. "/"
        .. string.gsub(
          value["path"],
          "([^0-9a-zA-Z%-_%.~/])",
          function(c)
            return string.format("%%%02X", string.byte(c))
          end
        )
      context["todo"][todo] = string.lower(value["hash"])
      check(todo .. "?authentication=" .. url_encode("Bearer " .. context["token"]))
    end
    for _, child in pairs(value) do
      if type(child) == "table" then
        walk_version(child, versionid)
      end
    end
  end

  local function queue_child_objects(value)
    if value["agentid"] and value["datasetid"] then
      check("https://data.world/" .. value["agentid"] .. "/" .. value["datasetid"])
    end
    if value["queryid"] then
      local base = "https://data.world/api/" .. context["owner"] .. "/dataset/" .. context["project"] .. "/queries/" .. value["queryid"]
      check(base)
      check(base .. "/versions?limit=50")
    end
    if value["insightid"] then
      local base = "https://data.world/api/" .. context["owner"] .. "/project/" .. context["project"] .. "/insights/" .. value["insightid"]
      check(base)
      check(base .. "/versions?limit=50")
      check(base .. "/posts")
    end
    for _, child in pairs(value) do
      if type(child) == "table" then
        queue_child_objects(child)
      end
    end
  end

  local function discover_search_projects(value)
    if value["agentid"] and value["datasetid"] then
      check("https://data.world/" .. value["agentid"] .. "/" .. value["datasetid"])
    end
    for key, child in pairs(value) do
      if type(child) == "table" then
        discover_search_projects(child)
      elseif key == "resource" or key == "resourceParent" then
        local project = string.match(child, "^dataset:([^/]+/[^/]+)$")
          or string.match(child, "^project:([^/]+/[^/]+)$")
        if project then
          check("https://data.world/" .. project)
        end
      end
    end
  end

  if allowed(url)
    and status_code < 300
    and item_type ~= "asset" then
    data = read_file(file)
    if url == "https://data.world/" .. item_value then
      local raw_config = string.match(data, "<script[^>]-id=\"init%-config\"[^>]*>(.-)</script>")
      if raw_config then
        local config = cjson.decode(raw_config)
        context["regional_cluster"] = config["regional_cluster"]
        local resources = config["openAccessResources"]
        if resources then
          local access = resources[context["owner"]] or {}
          local allowed = access["mode"] == "all"
          for _, project in ipairs(access["allowlist"] or {}) do
            if project == context["project"] then
              allowed = true
            end
          end
          if not allowed then
            io.stdout:write("No browser view.\n")
            io.stdout:flush()
            abort_item()
            return urls
          end
        end
        ids["https://data.world/api/events/token"] = true
        check(
          "https://data.world/api/events/token",
          {
            ["Cookie"]=config["csrf_cookie_name"] .. "=a",
            [config["csrf_header_name"]]="a"
          },
          "",
          "POST"
        )
      end
      context["token"] = string.match(data, "\"token\":\"([^\"]+)\"")
      if not context["token"] or not context["regional_cluster"] then
        abort_item()
        return urls
      end
      local dataset = "https://data.world/api/" .. context["owner"] .. "/dataset/" .. context["project"]
      check(dataset, {["Cookie"]="token=" .. context["token"]})
      check(
        "https://download.data.world/download/" .. item_value .. "?dwr=" .. url_encode(context["regional_cluster"]),
        {
          ["Content-Type"]="application/x-www-form-urlencoded",
          ["Origin"]="https://data.world",
          ["Referer"]=""
        },
        "authentication=Bearer+" .. context["token"],
        "POST"
      )
      check("https://data.world/" .. item_value .. "/activity")
      check("https://data.world/api/search?type=dataset&size=1&filters=agentid%3D" .. context["owner"] .. "%26datasetid%3D" .. context["project"])
      check("https://data.world/api/search?query=" .. url_encode("linksDataset:" .. item_value) .. "&size=100&from=0&type=dataset")
      check("https://data.world/api/search?boostQuery=&query=" .. url_encode("linksDataset:" .. item_value) .. "&size=3&type=dataset")
      check("https://data.world/api/search?query=" .. url_encode("linksDataset:" .. item_value) .. "&size=0&type=dataset")
      for _, suffix in ipairs({
        "?includeEnrichedLinks=false",
        "/versions",
        "/layers/extent",
        "/ontologies",
        "/queries",
        "/activity"
      }) do
        check(dataset .. suffix)
      end
      local insights = "https://data.world/api/" .. context["owner"] .. "/project/" .. context["project"] .. "/insights"
      check(insights)
      check(insights .. "/posts")
      return urls
    end
    if string.match(url, "^https?://query%.data%.world/table_view_window/") then
      local json = cjson.decode(data)
      if type(json["head"]) ~= "table"
        or type(json["metadata"]) ~= "table"
        or type(json["results"]) ~= "table" then
        abort_item()
      end
      return urls
    end
    if string.match(url, "^https?://data%.world/api/") then
      local json = cjson.decode(data)
      local found = find_item(url)
      if found and found["type"] == "search" then
        for _, search in ipairs({
          {"agent", "created"},
          {"dataset", "created"},
          {"datasetComment", "created"},
          {"file", "updated"},
          {"insight", "created"},
          {"query", "updated"}
        }) do
          check("https://data.world/api/search?type=" .. search[1] .. "&size=100&from=0&filters=" .. url_encode(search[2] .. "=" .. item_value))
        end
        local records = paginate_search(json)
        for _, record in ipairs(records) do
          discover_search_projects(record)
          scan_value(record)
        end
        return urls
      end
      if item_type == "project" then
        if url == "https://data.world/api/events/token" then
          check(
            "https://download.data.world/datapackage/" .. item_value .. "?dwr=" .. url_encode(context["regional_cluster"]),
            {
              ["Content-Type"]="application/x-www-form-urlencoded",
              ["Origin"]="https://data.world",
              ["Referer"]=""
            },
            "authentication=Bearer+" .. json["token"],
            "POST"
          )
        elseif url == "https://data.world/api/search?type=dataset&size=1&filters=agentid%3D" .. context["owner"] .. "%26datasetid%3D" .. context["project"] then
          for _, record in ipairs(json["records"]) do
            if record["_id"] == item_value then
              local title = record["name"] or context["project"]
              check("https://data.world/api/discussion/topics?agentid=" .. url_encode(context["owner"]) .. "&datasetid=" .. url_encode(context["project"]) .. "&type=dataset&title=" .. url_encode(title))
              if record["project"] == false then
                check(
                  "https://data.world/api/" .. context["owner"] .. "/dataset/" .. context["project"] .. "/related?ownerOnly=false&size=3",
                  {
                    ["Content-Type"]="application/json",
                    ["Cookie"]="_csrf=a",
                    ["x-csrf-token"]="a"
                  },
                  "{\"otherDatasets\":[]}",
                  "POST"
                )
              end
            end
          end
        elseif string.match(url, "^https?://data%.world/api/search%?")
          and string.match(url, "[%?&]query=([^&]+)") == url_encode("linksDataset:" .. item_value) then
          if string.match(url, "&size=100&") then
            paginate_search(json)
          end
        elseif url == "https://data.world/api/" .. context["owner"] .. "/dataset/" .. context["project"] .. "/versions" then
          local version = json[1]
          context["versionid"] = version["versionid"]
          for _, entry in ipairs(version["contents"]) do
            if not string.match(string.lower(entry["filename"]), "^%.dataworld/") then
              local digest = string.match(string.lower(entry["objectHash"]), "^etag:([0-9a-f]+)$")
              if digest and string.len(digest) == 32 then
                local download = "https://download.data.world/file_download/" .. item_value .. "/"
                  .. string.gsub(
                    entry["filename"],
                    "([^0-9a-zA-Z%-_%.~/])",
                    function(c)
                      return string.format("%%%02X", string.byte(c))
                    end
                  )
                  .. "?dwr=" .. url_encode(context["regional_cluster"])
                context["files"][download] = digest
                check(
                  download,
                  {
                    ["Content-Type"]="application/x-www-form-urlencoded",
                    ["Origin"]="https://data.world",
                    ["Referer"]=""
                  },
                  "authentication=Bearer+" .. context["token"],
                  "POST"
                )
              else
                io.stdout:write("Found unsupported hash.\n")
                io.stdout:flush()
                abort_item()
              end
            end
          end
          for _, version in ipairs(json) do
            check("https://data.world/api/" .. context["owner"] .. "/dataset/" .. context["project"] .. "/version/" .. version["versionid"])
          end
        elseif string.match(url, "^https?://data%.world/api/[^/]+/dataset/[^/]+/version/") then
          local version = json["version"]
          if version["versionid"] == context["versionid"] and version["computedViews"] then
            walk_version(version["computedViews"], version["versionid"])
          end
          if version["previousVersionid"] then
            check("https://data.world/api/" .. context["owner"] .. "/dataset/" .. context["project"] .. "/version/" .. version["previousVersionid"])
          end
        elseif url == "https://data.world/api/" .. context["owner"] .. "/dataset/" .. context["project"] .. "/layers/extent" then
          for _, layer in ipairs(json["tables"]) do
            check(
              "https://query.data.world/table_view_window/" .. item_value .. "/" .. url_encode(layer["@id"]) .. "?startRow=0&endRow=200&ascending=true",
              {
                ["Accept"]="application/sparql-results+json",
                ["Authorization"]="Bearer " .. context["token"],
                ["Origin"]="https://data.world",
                ["Referer"]="https://data.world/"
              }
            )
          end
        elseif string.match(url, "^https?://data%.world/api/discussion/topics%?") then
          for _, topic in ipairs(json) do
            if topic["id"] ~= "-" then
              check("https://data.world/api/discussion/posts?agentid=" .. url_encode(context["owner"]) .. "&datasetid=" .. url_encode(context["project"]) .. "&topicid=" .. url_encode(topic["id"]) .. "&type=dataset")
            end
          end
        elseif string.match(url, "/activity") and json["next"] then
          check("https://data.world/api/" .. context["owner"] .. "/dataset/" .. context["project"] .. "/activity?id_lt=" .. url_encode(json["next"]["id_lt"]) .. "&limit=" .. tostring(json["next"]["limit"] or 25))
        end
        if string.match(url, "/versions%?limit=50") and json["next"] then
          check(set_param(url, "next", url_encode(json["next"])))
        end
        queue_child_objects(json)
        scan_value(json)
      end
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  if status_code == 429
    and string.match(url["url"], "^https?://download%.data%.world/datapackage/") then
    retry_after = tonumber(http_stat["response_headers"]["headers"]["retry-after"][1])
  end
  if status_code ~= 200
    and status_code ~= 301
    and status_code ~= 302
    and not (
      item_type == "asset"
      and status_code == 403
    )
    and not (
      item_type == "project"
      and status_code == 404
      and (
        url["url"] == "https://data.world/api/" .. context["owner"] .. "/dataset/" .. context["project"] .. "?includeEnrichedLinks=false"
        or string.match(url["url"], "^https?://download%.data%.world/prebuilt_view/" .. string.gsub(item_value, "([^0-9a-zA-Z])", "%%%1") .. "/")
      )
    ) then
    retry_url = true
    return false
  end
  if status_code == 200 then
    local todo = string.match(url["url"], "^([^%?]+)")
    local file_download = context["files"] and context["files"][url["url"]]
    local expected = file_download or context["todo"] and context["todo"][todo]
    if http_stat["len"] == 0 and not expected then
      retry_url = true
      return false
    end
    if expected
      and string.match(expected, "^[0-9a-f]+$")
      and string.len(expected) == 32 then
      local digest = openssl_digest.new("md5")
      local file = assert(io.open(http_stat["local_file"], "rb"))
      while true do
        local data = file:read(1024 * 1024)
        if not data then
          break
        end
        digest:update(data)
      end
      file:close()
      local actual = string.gsub(digest:final(), ".", function(c)
        return string.format("%02x", string.byte(c))
      end)
      if actual ~= expected
        and string.match(todo, "/pbv%-global%-allFilesEntityRecommendations%.json$") then
        local data = read_file(http_stat["local_file"])
        data = string.gsub(data, ",\"dismissed\":true", "")
        data = string.gsub(data, ",\"dismissed\":false", "")
        digest = openssl_digest.new("md5")
        digest:update(data)
        actual = string.gsub(digest:final(), ".", function(c)
          return string.format("%02x", string.byte(c))
        end)
      end
      if actual ~= expected then
        retry_url = true
        return false
      end
      if file_download then
        context["files"][url["url"]] = nil
      end
    end
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local datapackage = string.match(url["url"], "^https?://download%.data%.world/datapackage/")
    local maxtries = 6
    if datapackage then
      maxtries = 60
    end
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time
    if datapackage then
      sleep_time = 5
      if retry_after then
        sleep_time = retry_after
      end
      retry_after = nil
    else
      sleep_time = math.random(
        math.floor(math.pow(2, tries-0.5)),
        math.floor(math.pow(2, tries))
      )
    end
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      if not seen_200[url["url"]] then
        seen_200[url["url"]] = 0
      end
      seen_200[url["url"]] = seen_200[url["url"]] + 1
    end
    downloaded[url["url"]] = true
  end

  if status_code == 301 or status_code == 302 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
    ids[string.lower(newloc)] = true
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  if item_type == "project" then
    for _ in pairs(context["files"]) do
      abort_item()
      break
    end
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["dataworld-xcntpsgg1dd50ea7"] = discovered_items,
    ["urls-mifbwshvuj9chv58"] = discovered_outlinks
  }) do
    print("queuing for", string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end
