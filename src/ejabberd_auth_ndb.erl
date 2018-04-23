%%%----------------------------------------------------------------------
%%% File    : ejabberd_auth_ndb.erl
%%% Author  : Muhammad Naeem <m.naemakram@gmail.com>
%%% Purpose : Authentification via ndb
%%% Created : 3 Aug 2017 by Muhammad Naeem <m.naemakram@gmail.com>
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

-module(ejabberd_auth_ndb).

-compile([{parse_transform, ejabberd_sql_pt}]).

-author('m.naemakram@gmail.com').

-behaviour(ejabberd_auth).

%% External exports
-export([start/1, stop/1, set_password/3, try_register/3,
	 get_users/2, count_users/2,
	 get_password/2, remove_user/2, store_type/1, export/1, import/2,
	 plain_password_required/1]).

-include("ejabberd.hrl").
-include("ejabberd_sql_pt.hrl").

%%-record(passwd, {us = {<<"">>, <<"">>} :: {binary(), binary()} | '$1',
%%                 password = <<"">> :: binary() | scram() | '_'}).

start(_Host) ->
    ok.

stop(_Host) ->
    ok.

plain_password_required(Server) ->
    store_type(Server) == scram.

store_type(Server) ->
    ejabberd_auth:password_format(Server).

get_users(Server, _) ->
    case ejabberd_ndb:get_all_users(Server) of
        {ok, Users} ->
            Users;
        _ ->
            []
    end.

count_users(Server, _) ->
    case ejabberd_ndb:get_users_count(Server) of
        {ok, N} ->
            N;
        _ ->
            0
    end.

get_password(User, Server) ->
	case ejabberd_ndb:get_user_password(User, Server) of
	{ok, Password} ->
	    {ok, Password};
	_ ->
	    error
    end.

%%Upating User Password	
set_password(User, _Server, Password) ->
    ejabberd_ndb:set_user_password(User, Password),
    ok.

try_register(User, _Server, Password) ->
	ejabberd_ndb:add_user_password(User, Password),
    ok.

%%Remove User
remove_user(User, Server) ->
    ejabberd_ndb:del_user(User, Server).

export(_Server) ->
	% Not Implemented
    [].

import(_, _) ->
    % Not Implemented
    ok.
