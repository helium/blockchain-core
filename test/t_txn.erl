-module(t_txn).

-export_type([
    t/0
]).

-export([
    pay/4,
    gateway_add/2,
    assert_location/4
]).

-type t() :: blockchain_txn:txn().

-spec pay(t_user:t(), t_user:t(), non_neg_integer(), non_neg_integer()) ->
    t().
pay(Src, Dst, Amount, Nonce) ->
    blockchain_txn_payment_v1:sign(
        blockchain_txn_payment_v1:new(
            t_user:addr(Src),
            t_user:addr(Dst),
            Amount,
            Nonce
        ),
        t_user:sig_fun(Src)
    ).

-spec gateway_add(t_user:t(), t_user:t()) ->
    t().
gateway_add(Owner, Gateway) ->
    Tx1 = blockchain_txn_add_gateway_v1:new(t_user:addr(Owner), t_user:addr(Gateway)),
    Tx2 = blockchain_txn_add_gateway_v1:sign(Tx1, t_user:sig_fun(Owner)),
    Tx3 = blockchain_txn_add_gateway_v1:sign_request(Tx2, t_user:sig_fun(Gateway)),
    Tx3.

-spec assert_location(t_user:t(), t_user:t(), non_neg_integer(), pos_integer()) ->
    t().
assert_location(Owner, Gateway, Index, Nonce) ->
    Tx1 =
        blockchain_txn_assert_location_v1:new(
            t_user:addr(Gateway),
            t_user:addr(Owner),
            Index,
            Nonce
        ),
    Tx2 = blockchain_txn_assert_location_v1:sign_request(Tx1, t_user:sig_fun(Gateway)),
    Tx3 = blockchain_txn_assert_location_v1:sign(Tx2, t_user:sig_fun(Owner)),
    Tx3.
