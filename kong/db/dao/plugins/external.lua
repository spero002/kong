local cjson = require "cjson.safe"
local ngx_ssl = require "ngx.ssl"
local pl_path = require "pl.path"
local raw_log = require "ngx.errlog".raw_log

local rpc = require "kong.db.dao.plugins.mp_rpc"

local ngx = ngx
local kong = kong
local unpack = unpack
local ngx_INFO = ngx.INFO
local ngx_timer_at = ngx.timer.at
local cjson_encode = cjson.encode
local cjson_decode = cjson.decode

--- module table
local external_plugins = {}


local _servers
local _plugin_infos

--- keep request data a bit longer, into the log timer
local save_for_later = {}

--- handle notifications from pluginservers
local rpc_notifications = {}

--- currently running plugin instances
local running_instances = {}


--[[

Configuration

The external_plugins_config YAML file defines a list of plugin servers.  Each one
can have the following fields:

name: (required) a unique string.  Shouldn't collide with Lua packages available as `kong.plugins.<name>`
socket: (required) path of a unix domain socket to use for RPC.
exec: (optional) executable file of the server.  If omitted, the server process won't be managed by Kong.
args: (optional) a list of strings to be passed as command line arguments.
env: (optional) a {string:string} map to be passed as environment variables.
info_cmd: (optional) command line to request plugin info.

--]]

local function ifexists(path)
  if pl_path.exists(path) then
    return path
  end
end


local function get_server_defs()
  local config = kong.configuration

  if not _servers then
    _servers = {}
    for i, name in ipairs(config.pluginserver_names or {}) do
      kong.log.debug("search config for pluginserver named: ", name)
      local env_prefix = "pluginserver_" .. name
      _servers[i] = {
        name = name,
        socket = config[env_prefix .. "_socket"] or "/usr/local/kong/" .. name .. ".socket",
        start_command = config[env_prefix .. "_start_cmd"] or ifexists("/usr/local/bin/"..name),
        query_command = config[env_prefix .. "_query_cmd"] or ifexists("/usr/local/bin/query_"..name),
      }
    end
  end

  return _servers
end


local function get_server_rpc(server_def)
  if not server_def.rpc then
    server_def.rpc = rpc.new(server_def.socket, rpc_notifications)
    --kong.log.debug("server_def: ", server_def, "   .rpc: ", server_def.rpc)
  end

  return server_def.rpc
end

--[[

RPC

Each plugin server specifies a socket path to communicate.  Protocol is the same
as Go plugins.

CONSIDER:

- when a plugin server notifies a new PID, Kong should request all plugins info again.
  Should it use RPC at this time, instead of commandline?

- Should we add a new notification to ask kong to request plugin info again?

--]]




--[[

Instance_id/conf   relation

--]]


--- get_instance_id: gets an ID to reference a plugin instance running in a
--- pluginserver each configuration in the database is handled by a different
--- instance.  Biggest complexity here is due to the remote (and thus non-atomic
--- and fallible) operation of starting the instance at the server.
local function get_instance_id(plugin_name, conf)
  local key = type(conf) == "table" and conf.__key__ or plugin_name
  local instance_info = running_instances[key]

  while instance_info and not instance_info.id do
    -- some other thread is already starting an instance
    ngx.sleep(0)
    instance_info = running_instances[key]
  end

  if instance_info
    and instance_info.id
    and instance_info.seq == conf.__seq__
  then
    -- exact match, return it
    return instance_info.id
  end

  local old_instance_id = instance_info and instance_info.id
  if not instance_info then
    -- we're the first, put something to claim
    instance_info          = {
      conf = conf,
      seq = conf.__seq__,
    }
    running_instances[key] = instance_info
  else

    -- there already was something, make it evident that we're changing it
    instance_info.id = nil
  end

  local plugin_info = _plugin_infos[plugin_name]
  local server_rpc  = get_server_rpc(plugin_info.server_def)

  local status, err = server_rpc:call("plugin.StartInstance", {
    Name = plugin_name,
    Config = cjson_encode(conf)
  })
  if status == nil then
    kong.log.err("starting instance: ", err)
    -- remove claim, some other thread might succeed
    running_instances[key] = nil
    error(err)
  end

  instance_info.id = status.Id
  instance_info.conf = conf
  instance_info.seq = conf.__seq__
  instance_info.Config = status.Config
  instance_info.rpc = server_rpc

  if old_instance_id then
    -- there was a previous instance with same key, close it
    server_rpc:call("plugin.CloseInstance", old_instance_id)
    -- don't care if there's an error, maybe other thread closed it first.
  end

  return status.Id
end

--- reset_instance: removes an instance from the table.
local function reset_instance(plugin_name, conf)
  local key = type(conf) == "table" and conf.__key__ or plugin_name
  running_instances[key] = nil
end


--- serverPid notification sent by the pluginserver.  if it changes,
--- all instances tied to this RPC socket should be restarted.
function rpc_notifications:serverPid(n)
  n = tonumber(n)
  if self.pluginserver_pid and n ~= self.pluginserver_pid then
    for key, instance in pairs(running_instances) do
      if instance.rpc == self then
        running_instances[key] = nil
      end
    end
  end

  self.pluginserver_pid = n
end



--[[

Exposed API

--]]


-- global method search and cache
local function index_table(table, field)
  if table[field] then
    return table[field]
  end

  local res = table
  for segment, e in ngx.re.gmatch(field, "\\w+", "o") do
    if res[segment[0]] then
      res = res[segment[0]]
    else
      return nil
    end
  end
  return res
end


local get_field
do
  local exposed_api = {
    kong = kong,

    ["kong.log.serialize"] = function()
      local saved = save_for_later[coroutine.running()]
      return cjson_encode(saved and saved.serialize_data or kong.log.serialize())
    end,

    ["kong.nginx.get_var"] = function(v)
      return ngx.var[v]
    end,

    ["kong.nginx.get_tls1_version_str"] = ngx_ssl.get_tls1_version_str,

    ["kong.nginx.get_ctx"] = function(k)
      local saved = save_for_later[coroutine.running()]
      local ngx_ctx = saved and saved.ngx_ctx or ngx.ctx
      return ngx_ctx[k]
    end,

    ["kong.nginx.set_ctx"] = function(k, v)
      local saved = save_for_later[coroutine.running()]
      local ngx_ctx = saved and saved.ngx_ctx or ngx.ctx
      ngx_ctx[k] = v
    end,

    ["kong.ctx.shared.get"] = function(k)
      local saved = save_for_later[coroutine.running()]
      local ctx_shared = saved and saved.ctx_shared or kong.ctx.shared
      return ctx_shared[k]
    end,

    ["kong.ctx.shared.set"] = function(k, v)
      local saved = save_for_later[coroutine.running()]
      local ctx_shared = saved and saved.ctx_shared or kong.ctx.shared
      ctx_shared[k] = v
    end,

    ["kong.nginx.req_start_time"] = ngx.req.start_time,

    ["kong.request.get_query"] = function(max)
      return rpc.fix_mmap(kong.request.get_query(max))
    end,

    ["kong.request.get_headers"] = function(max)
      return rpc.fix_mmap(kong.request.get_headers(max))
    end,

    ["kong.response.get_headers"] = function(max)
      return rpc.fix_mmap(kong.response.get_headers(max))
    end,

    ["kong.service.response.get_headers"] = function(max)
      return rpc.fix_mmap(kong.service.response.get_headers(max))
    end,
  }

  local method_cache = {}

  function get_field(method)
    if method_cache[method] then
      return method_cache[method]

    else
      method_cache[method] = index_table(exposed_api, method)
      return method_cache[method]
    end
  end
end


local function call_pdk_method(cmd, args)
  local method = get_field(cmd)
  if not method then
    kong.log.err("could not find pdk method: ", cmd)
    return
  end

  if type(args) == "table" then
    return method(unpack(args))
  end

  return method(args)
end


-- return objects via the appropriately typed StepXXX method
local get_step_method
do
  local by_pdk_method = {
    ["kong.client.get_credential"] = "plugin.StepCredential",
    ["kong.client.load_consumer"] = "plugin.StepConsumer",
    ["kong.client.get_consumer"] = "plugin.StepConsumer",
    ["kong.client.authenticate"] = "plugin.StepCredential",
    ["kong.node.get_memory_stats"] = "plugin.StepMemoryStats",
    ["kong.router.get_route"] = "plugin.StepRoute",
    ["kong.router.get_service"] = "plugin.StepService",
    ["kong.request.get_query"] = "plugin.StepMultiMap",
    ["kong.request.get_headers"] = "plugin.StepMultiMap",
    ["kong.response.get_headers"] = "plugin.StepMultiMap",
    ["kong.service.response.get_headers"] = "plugin.StepMultiMap",
  }

  function get_step_method(step_in, pdk_res, pdk_err)
    if not pdk_res and pdk_err then
      return "plugin.StepError", pdk_err
    end

    return ((type(pdk_res) == "table" and pdk_res._method)
      or by_pdk_method[step_in.Data.Method]
      or "plugin.Step"), pdk_res
  end
end



--[[

--- Event loop -- instance reconnection

--]]

local function bridge_loop(instance_rpc, instance_id, phase)
  if not instance_rpc then
    kong.log.err("no instance_rpc: ", debug.traceback())
  end
  local step_in, err = instance_rpc:call("plugin.HandleEvent", {
    InstanceId = instance_id,
    EventName = phase,
  })
  if not step_in then
    return step_in, err
  end

  local event_id = step_in.EventId

  while true do
    if step_in.Data == "ret" then
      break
    end

    local pdk_res, pdk_err = call_pdk_method(
      step_in.Data.Method,
      step_in.Data.Args)

    local step_method, step_res = get_step_method(step_in, pdk_res, pdk_err)

    step_in, err = instance_rpc:call(step_method, {
      EventId = event_id,
      Data = step_res,
    })
    if not step_in then
      return step_in, err
    end
  end
end


local function handle_event(instance_rpc, plugin_name, conf, phase)
  local instance_id = get_instance_id(plugin_name, conf)
  local _, err = bridge_loop(instance_rpc, instance_id, phase)

  if err then
    kong.log.err(err)

    if string.match(err, "No plugin instance") then
      reset_instance(plugin_name, conf)
      return handle_event(instance_rpc, plugin_name, conf, phase)
    end
  end
end


--- Phase closures
local function build_phases(plugin)
  if not plugin then
    return
  end

  local server_rpc = get_server_rpc(plugin.server_def)

  for _, phase in ipairs(plugin.phases) do
    if phase == "log" then
      plugin[phase] = function(self, conf)
        local saved = {
          serialize_data = kong.log.serialize(),
          ngx_ctx = ngx.ctx,
          ctx_shared = kong.ctx.shared,
        }

        ngx_timer_at(0, function()
          local co = coroutine.running()
          save_for_later[co] = saved

          handle_event(server_rpc, self.name, conf, phase)

          save_for_later[co] = nil
        end)
      end

    else
      plugin[phase] = function(self, conf)
        handle_event(server_rpc, self.name, conf, phase)
      end
    end
  end

  return plugin
end


--[[

Plugin info requests

Disclaimer:  The best way to do it is to have "ListPlugins()" and "GetInfo(plugin)"
RPC methods; but Kong would like to have all the plugin schemas at initialization time,
before full cosocket is available.  At one time, we used blocking I/O to do RPC at
non-yielding phases, but was considered dangerous.  The alternative is to use
`io.popen(cmd)` to ask fot that info.


In the external plugins configuration, the `.info_cmd` field contains a string
to be executed as a command line.  The output should be a JSON string that decodes
as an array of objects, each defining the name, priority, version and schema of one
plugin.

    [{
      "name": ... ,
      "priority": ... ,
      "version": ... ,
      "schema": ... ,
      "phases": [ phase_names ... ],
    },
    {
      ...
    },
    ...
    ]

This array should describe all plugins currently available through this server,
no matter if actually enabled in Kong's configuration or not.

--]]


local function register_plugin_info(server_def, plugin_info)
  if _plugin_infos[plugin_info.Name] then
    kong.log.err(string.format("Duplicate plugin name [%s] by %s and %s",
      plugin_info.Name, _plugin_infos[plugin_info.Name].server_def.name, server_def.name))
    return
  end

  _plugin_infos[plugin_info.Name] = {
    server_def = server_def,
    --rpc = server_def.rpc,
    name = plugin_info.Name,
    PRIORITY = plugin_info.Priority,
    VERSION = plugin_info.Version,
    schema = plugin_info.Schema,
    phases = plugin_info.Phases,
  }
end

local function ask_info(server_def)
  if not server_def.query_command then
    kong.log.info(string.format("No info query for %s", server_def.name))
    return
  end

  local fd, err = io.popen(server_def.query_command)
  if not fd then
    local msg = string.format("loading plugins info from [%s]:\n", server_def.name)
    kong.log.err(msg, err)
    return
  end

  local infos_dump = fd:read("*a")
  fd:close()
  local infos = cjson_decode(infos_dump)
  if type(infos) ~= "table" then
    error(string.format("Not a plugin info table: \n%s\n%s",
        server_def.query_command, infos_dump))
    return
  end

  for _, plugin_info in ipairs(infos) do
    register_plugin_info(server_def, plugin_info)
  end
end

local function load_all_infos()
  if not _plugin_infos then
    _plugin_infos = {}

    for _, server_def in ipairs(get_server_defs()) do
      ask_info(server_def)
    end
  end

  return _plugin_infos
end


local loaded_plugins = {}

local function get_plugin(plugin_name)
  if not loaded_plugins[plugin_name] then
    local plugin = load_all_infos()[plugin_name]
    loaded_plugins[plugin_name] = build_phases(plugin)
  end

  return loaded_plugins[plugin_name]
end

function external_plugins.load_plugin(plugin_name)
  local plugin = get_plugin(plugin_name)
  if plugin and plugin.PRIORITY then
    return true, plugin
  end

  return false, "no plugin found"
end

function external_plugins.load_schema(plugin_name)
  local plugin = get_plugin(plugin_name)
  if plugin and plugin.PRIORITY then
    return true, plugin.schema
  end

  return false, "no plugin found"
end


--[[

Process management

Servers that specify an `.exec` field are launched and managed by Kong.
This is an attempt to duplicate the smallest reasonable subset of systemd.

Each process specifies executable, arguments and environment.
Stdout and stderr are joined and logged, if it dies, Kong logs the event
and respawns the server.

--]]

local function grab_logs(proc, name)
  local prefix = string.format("[%s:%d] ", name, proc:pid())

  while true do
    local data, err, partial = proc:stdout_read_line()
    local line = data or partial
    if line and line ~= "" then
      raw_log(ngx_INFO, prefix .. line)
    end

    if not data and err == "closed" then
      return
    end
  end
end

local function handle_server(server_def)
  if not server_def.socket then
    -- no error, just ignore
    return
  end

  if server_def.start_command then
    ngx_timer_at(0, function(premature)
      if premature then
        return
      end

      local ngx_pipe = require "ngx.pipe"

      while not ngx.worker.exiting() do
        kong.log.notice("Starting " .. server_def.name or "")
        server_def.proc = assert(ngx_pipe.spawn(server_def.start_command, {
          merge_stderr = true,
        }))
        server_def.proc:set_timeouts(nil, nil, nil, 0)     -- block until something actually happens

        while true do
          grab_logs(server_def.proc, server_def.name)
          local ok, reason, status = server_def.proc:wait()
          if ok ~= nil or reason == "exited" then
            kong.log.notice("external pluginserver '", server_def.name, "' terminated: ", tostring(reason), " ", tostring(status))
            break
          end
        end
      end
      kong.log.notice("Exiting: go-pluginserver not respawned.")
    end)
  end

  return server_def
end

function external_plugins.manage_servers()
  if ngx.worker.id() ~= 0 then
    kong.log.notice("only worker #0 can manage")
    return
  end

  for i, server_def in ipairs(get_server_defs()) do
    local server, err = handle_server(server_def)
    if not server then
      kong.log.err(err)
    end
  end
end

return external_plugins
