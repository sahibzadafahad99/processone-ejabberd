%%%----------------------------------------------------------------------
%%% File    : ejabberd_ndb_sup.erl
%%% Author  : Muhammad Naeem <m.naemakram@gmail.com>
%%% Purpose : ndb connections supervisor
%%% Created : 25 Oct 2017 by Muhammad Naeem <m.naemakram@gmail.com>
%%%
%%%
%%% lynk-ejabberd, Copyright (C) 2017   Whizpool
%%%
%%% This program is free software; you can redistribute it and/or
%%% modify it under the terms of the GNU General Public License as
%%% published by the Free Software Foundation; either version 2 of the
%%% License, or (at your option) any later version.
%%%
%%% This program is distributed in the hope that it will be useful,
%%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
%%% General Public License for more details.
%%%
%%% You should have received a copy of the GNU General Public License along
%%% with this program; if not, write to the Free Software Foundation, Inc.,
%%% 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
%%%
%%%----------------------------------------------------------------------


-module(ejabberd_ndb_sup).

-behaviour(supervisor).
-behaviour(ejabberd_config).
-author('alexey@process-one.net').

-export([start_link/0, init/1, get_pids/0,
	 transform_options/1, get_random_pid/0, get_random_pid/1]).

-include("ejabberd.hrl").
-include("logger.hrl").

-define(DEFAULT_POOL_SIZE, 10).
-define(DEFAULT_ndb_START_INTERVAL, 30). % 30 seconds
-define(DEFAULT_ndb_HOST, "127.0.0.1").
-define(DEFAULT_ndb_PORT, 8087).

% time to wait for the supervisor to start its child before returning
% a timeout error to the request
-define(CONNECT_TIMEOUT, 500). % milliseconds

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("erlcass.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
	ok.

get_pids() ->
    [get_random_pid(p1_time_compat:monotonic_time())].

get_random_pid() ->
    get_random_pid(p1_time_compat:monotonic_time()).

get_random_pid(_) ->
    get_random_pid(p1_time_compat:monotonic_time()).

transform_options(Opts) ->
    lists:foldl(fun transform_options/2, [], Opts).

transform_options({ndb_server, {S, P}}, Opts) ->
    [{ndb_server, S}, {ndb_port, P}|Opts];
transform_options(Opt, Opts) ->
    [Opt|Opts].

