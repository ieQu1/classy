%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(CLASSY_HRL).
-define(CLASSY_HRL, true).

%% Peer membership status.
%%
%% Let L be the site ID of the local node and R be id of site described by the record...
-record(member_s_v0,
        { %% Event order of the last command that updated state of R on L.
          %% `undefined' if state is fresh.
          %%
          %% Note: `undefined` < {}.
          ord :: classy_membership:event_order() | undefined
          %% Logical clock:
        , ts = 0
          %% Index of the last processed R's log entry:
        , ii = 0 :: classy_membership:clock()
          %% Is node member of the cluster?
        , mem = false :: boolean()
        }).

-endif.
