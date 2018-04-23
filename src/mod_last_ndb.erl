%%%-------------------------------------------------------------------
%%% File    : mod_last_ndb.erl
%%% Author  : Muhammad Naeem <m.naemakram@gmail.com>
%%% Created : 26 Aug 2017 by Muhammad Naeem <m.naemakram@gmail.com>
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

-module(mod_last_ndb).

-behaviour(mod_last).

%% API
-export([init/2, import/2, get_last/2, store_last_info/4, remove_user/2]).

-include("mod_last.hrl").
-include("logger.hrl").

%%%===================================================================
%%% API
%%%===================================================================
init(_Host, _Opts) ->
    ok.

get_last(LUser, LServer) ->
    case ejabberd_ndb:get_last(LUser, LServer) of
        {ok, #last_activity{timestamp = TimeStamp,
                            status = Status}} ->
            {ok, {TimeStamp, Status}};
	{error, notfound} ->
	    error;
        _Err ->
	    %% TODO: log error
	    {error, db_failure}
    end.

store_last_info(LUser, LServer, TimeStamp, Status) ->
    %%US = {LUser, LServer},	
	%%LastActivity = #last_activity{us = US,timestamp = TimeStamp,status = Status},
    ejabberd_ndb:store_last_info(LUser, LServer, TimeStamp, Status).

remove_user(_LUser, _LServer) ->
    %{atomic, ejabberd_riak:delete(last_activity, {LUser, LServer})}.
    {atomic, ok}.

import(_,_) ->
    ok.
