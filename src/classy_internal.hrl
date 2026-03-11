%%--------------------------------------------------------------------
%% Copyright (c) 2024, 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-ifndef(CLASSY_INTERNAL_HRL).
-define(CLASSY_INTERNAL_HRL, true).

-record(classy_kv, {k, v}).

-define(on_node_init, on_node_init).
-define(on_create_cluster, on_create_cluster).
-define(on_create_site, on_create_site).
-define(on_site_status_change, on_site_status_change).
-define(on_membership_change, on_membership_change).
-define(on_pre_join, on_pre_join).
-define(on_post_join, on_post_join).
-define(on_pre_kick, on_pre_kick).
-define(on_post_leave, on_post_leave).

-endif.
