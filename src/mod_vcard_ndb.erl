%%%-------------------------------------------------------------------
%%% File    : mod_vcard_ndb.erl
%%% Author  : Muhammad Naeem <m.naemakram@gmail.com>
%%% Created : 3 Aug, 2017 Muhammad Naeem <m.naemakram@gmail.com>
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

-module(mod_vcard_ndb).

-behaviour(mod_vcard).

%% API
-export([init/2, get_vcard/2, set_vcard/4, search/4, remove_user/2,
	 search_fields/1, search_reported/1, import/3, stop/1]).
-export([is_search_supported/1]).

-include("ejabberd.hrl").
-include("xmpp.hrl").
-include("mod_vcard.hrl").
-include("logger.hrl").

%%%===================================================================
%%% API
%%%===================================================================
init(_Host, _Opts) ->
   ok.

stop(_Host) ->
    ok.

is_search_supported(_ServerHost) ->
    false.


get_vcard(LUser, LServer) ->
    case ejabberd_ndb:get_vcard(LUser, LServer) of
	{ok, SVCARD} ->
	   {ok, [SVCARD]};
	{selected, []} -> {ok, []};
	_ -> error
    end.

	
%%%===================================================================
%%% As we required our own fields for the user profiles so we have modified this function.
%%% Default function paramaters using ejabberd /xmpp has been overwritten here.
%%% Please don't use the default paramaters for this functions.
%%%===================================================================

set_vcard(LUser, LServer, VCARD, _VCardSearch) ->
    Type = fxml:get_subtag_cdata(VCARD, <<"type">>),
    Name = fxml:get_subtag_cdata(VCARD, <<"name">>),
    Image = fxml:get_subtag_cdata(VCARD, <<"image">>),    
    Storage = fxml:get_subtag_cdata(VCARD, <<"storage">>),    
    Status = fxml:get_subtag_cdata(VCARD, <<"status">>),
	Stickers = fxml:get_subtag_cdata(VCARD, <<"stickers">>),
	SeenReceipt = fxml:get_subtag_cdata(VCARD, <<"seen_receipt">>),
	SharedWallpapers = fxml:get_subtag_cdata(VCARD, <<"sharedwallpapers">>),
	ImageUpdate = fxml:get_subtag_cdata(VCARD, <<"imageupdate">>),						
    PublicInfo = fxml:get_subtag_cdata(VCARD, <<"publicinfo">>),    
    ejabberd_ndb:set_vcard(LUser,LServer,Type, Name, Image, Status,Stickers,SeenReceipt,SharedWallpapers,ImageUpdate,PublicInfo,Storage ).



	
search(_LServer, _Data, _AllowReturnAll, _MaxMatch) ->
    % Not Implemented.
    ok.

search_fields(_LServer) ->
    [].

search_reported(_LServer) ->
    [].

remove_user(_LUser, _LServer) ->
    % Not Implemented
	ok.
import(_, _, _) ->
	% Not Implemented
    ok.	
