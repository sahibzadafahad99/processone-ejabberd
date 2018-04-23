%%%-------------------------------------------------------------------
%%% File    : mod_muc_ndb.erl
%%% Author  : Muhammad Naeem <m.naemakram@gmail.com>
%%% Created : 7 Aug 2017 by Muhammad Naeem <m.naemakram@gmail.com>
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

-module(mod_muc_ndb).

-behaviour(mod_muc).
-behaviour(mod_muc_room).

%% API
-export([init/2, import/3, store_room/5, restore_room/3, forget_room/3,
	 can_use_nick/4, get_rooms/2, get_nick/3, set_nick/4]).
-export([register_online_room/4, unregister_online_room/4, find_online_room/3,
	 get_online_rooms/3, count_online_rooms/2, rsm_supported/0,
	 register_online_user/4, unregister_online_user/4,
	 count_online_rooms_by_user/3, get_online_rooms_by_user/3,get_subscribed_rooms/3]).
-export([set_affiliation/6, set_affiliations/4, get_affiliation/5,
	 get_affiliations/3, search_affiliation/4]).

-include("jid.hrl").
-include("mod_muc.hrl").
-include("logger.hrl").

%%%===================================================================
%%% API
%%%===================================================================
init(_Host, _Opts) ->
    ok.

store_room(_LServer, Host, Name, Opts,_) ->
	%%Fetch Relevant Info from the Muc Opts to save in the database
	Title = proplists:get_value(title, Opts),
	Desc  = proplists:get_value(description, Opts),
	Subsc   = proplists:get_value(subscribers, Opts),
	Affs  = proplists:get_value(affiliations, Opts),
	SubList = ejabberd_ndb:subcribers_to_scylla(Subsc),	
	MucAff = ejabberd_ndb:affiliations_to_scylla(Affs),	
	
	%Removed Fetched Relevant Info from the Muc Opts as we saved these values separate 
	TOpts = proplists:delete(title, Opts),
	DOpts = proplists:delete(description, TOpts),
	AFOpts = proplists:delete(affiliations, DOpts),
	SubOpts = proplists:delete(subscribers, AFOpts),
	
	SOpts = misc:term_to_expr(SubOpts),
    {atomic, ejabberd_ndb:store_room(Name, Host, SOpts, Title,Desc,MucAff,SubList)}.

restore_room(_LServer, Host, Name) ->
    case ejabberd_ndb:restore_room(Name, Host) of
        {ok, #muc_room{opts = Opts}} -> misc:term_to_expr(Opts);
        _ -> error
    end.

forget_room(_LServer, Host, Name) ->
    {atomic, ejabberd_ndb:forget_room(Name, Host)}.

can_use_nick(LServer, Host, JID, Nick) ->
    {LUser, LServer, _} = jid:tolower(JID),
    LUS = {LUser, LServer},
    case ejabberd_ndb:get_nick(Nick, Host) of
        {ok, []} ->
            true;
        {ok, [#muc_registered{us_host = {U, _Host}}]} ->
            U == LUS;
        {error, _} ->
            true
    end.

get_rooms(_LServer, Host) ->
    case catch ejabberd_ndb:get_rooms(Host) of
	{ok, RoomOpts} ->
		
		lists:map(
	      fun(Rs) ->
			[Room, Opts, Title, Description, Affiliation,Subscribers] = Rs,
			#muc_room{name_host = {Room, Host},opts = ejabberd_ndb:make_muc_opt(Opts, Title, Description, Affiliation,Subscribers, Room)}		
	      end, RoomOpts);
	_Err ->
	    []
    end.

get_nick(_LServer, _Host, _From) ->
    % TODO
	ok.

set_nick(_LServer, _Host, _From, _Nick) ->
   % TODO
   ok.

set_affiliation(_ServerHost, _Room, _Host, _JID, _Affiliation, _Reason) ->
    {error, not_implemented}.

set_affiliations(_ServerHost, _Room, _Host, _Affiliations) ->
    {error, not_implemented}.

get_affiliation(_ServerHost, _Room, _Host, _LUser, _LServer) ->
    {error, not_implemented}.

get_affiliations(_ServerHost, _Room, _Host) ->
    {error, not_implemented}.

search_affiliation(_ServerHost, _Room, _Host, _Affiliation) ->
    {error, not_implemented}.

register_online_room(_, _, _, _) ->
    erlang:error(not_implemented).

unregister_online_room(_, _, _, _) ->
    erlang:error(not_implemented).

find_online_room(_, _, _) ->
    erlang:error(not_implemented).

count_online_rooms(_, _) ->
    erlang:error(not_implemented).

get_online_rooms(_, _, _) ->
    erlang:error(not_implemented).

rsm_supported() ->
    false.

register_online_user(_, _, _, _) ->
    erlang:error(not_implemented).

unregister_online_user(_, _, _, _) ->
    erlang:error(not_implemented).

count_online_rooms_by_user(_, _, _) ->
    erlang:error(not_implemented).

get_online_rooms_by_user(_, _, _) ->
    erlang:error(not_implemented).

get_subscribed_rooms(_, _, _) ->
    not_implemented.
	
import(_, _, _) ->
    ok.