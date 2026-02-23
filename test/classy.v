Require Import
  Orders
  List
  Permutation
  Logic.ProofIrrelevance.

Import ListNotations.

From Hammer Require Import Tactics.

Section compares.
  Context `{StrictOrder}.

  Inductive compares a b : Set :=
  | comp_eq : a = b -> compares a b
  | comp_lt : R a b -> compares a b
  | comp_gt : R b a -> compares a b.
End compares.

Class StrictOrderDec `{Hord : StrictOrder} :=
  { compare_dec : forall a b, @compares _ R a b
  }.

Global Arguments StrictOrderDec (A _).

Infix "<?>" := compare_dec (at level 50).

Section merge.
  Context {LogEntry Ord : Set} `{Hdecord : StrictOrderDec Ord}.

  Parameter ord : LogEntry -> Ord.

  Definition merge0 a b :=
    match ord a <?> ord b with
    | comp_lt _ _ _ => b
    | _ => a
    end.

  Lemma Rab_to_ord a b:
    R a b ->
    exists H, a <?> b = comp_lt a b H.
  Proof.
    intros H.
    destruct Hord as [Hirr Htran].
    destruct (a <?> b).
    - exfalso. subst. now apply Hirr in H.
    - now exists r.
    - exfalso. specialize (Htran _ _ _ H r) as H1. now apply Hirr in H1.
  Qed.

  Lemma Rba_to_ord a b:
    R b a ->
    exists H, a <?> b = comp_gt a b H.
  Proof.
    intros H.
    destruct Hord as [Hirr Htran].
    destruct (a <?> b).
    - exfalso. subst. now apply Hirr in H.
    - exfalso. specialize (Htran _ _ _ H r) as H1. now apply Hirr in H1.
    - now exists r.
  Qed.

  Lemma merge0_refl a : merge0 a a = a.
  Proof.
    sauto unfold:merge0.
  Qed.

  Definition proper_pair a b :=
    ord a = ord b ->
    a = b.

  Lemma merge0_lt a b :
    proper_pair a b ->
    R (ord a) (ord b) ->
    merge0 a b = b.
  Proof.
    intros Hp H.
    apply Rab_to_ord in H.
    destruct H as [H Hl].
    unfold merge0. now rewrite Hl.
  Qed.

  Lemma merge0_gt a b :
    proper_pair a b ->
    R (ord b) (ord a) ->
    merge0 a b = a.
  Proof.
    intros Hp H.
    apply Rba_to_ord in H.
    destruct H as [H Hl].
    unfold merge0. now rewrite Hl.
  Qed.

  Lemma merge0_symm a b :
    proper_pair a b ->
    merge0 a b = merge0 b a.
  Proof.
    intros Hproper.
    unfold merge0.
    remember (ord a <?> ord b) as ab.
    remember (ord b <?> ord a) as ba.
    destruct Hord as [Hirr Htran].
    destruct ab as [ab|ab|ab]; destruct ba as [ba|ba|ba];
      subst; try apply Hproper; try easy;
      specialize (Htran _ _ _ ab ba) as Hcontr;
      now apply Hirr in Hcontr.
  Qed.

  Ltac rev_eq :=
    let H1 := fresh in
    let H2 := fresh in
    progress match goal with
    | [ H1 : ord ?a = ord ?b, H2 : proper_pair ?a ?b |- _ ] =>
        apply H2 in H1;
        subst;
        repeat rewrite merge0_refl
    end.

  Ltac rev_lt :=
    let H1 := fresh in
    let H2 := fresh in
    progress match goal with
    | [ H1 : R (ord ?a) (ord ?b), H2 : proper_pair ?a ?b |- _ ] =>
        rewrite (merge0_lt a b H2 H1)
    end.

  Ltac rev_gt :=
    let H1 := fresh in
    let H2 := fresh in
    progress match goal with
    | [ H1 : R (ord ?b) (ord ?a), H2 : proper_pair ?a ?b |- _ ] =>
        rewrite (merge0_gt a b H2 H1)
    end.

  Lemma merge0_assoc a b c :
    proper_pair a b ->
    proper_pair b c ->
    proper_pair a c ->
    merge0 (merge0 a b) c = merge0 a (merge0 b c).
  Proof with repeat (rev_eq || rev_lt || rev_gt); try rewrite merge0_refl; try easy.
    intros Hproper_ab Hproper_bc Hproper_ac.
    unfold max.
    destruct (ord a <?> ord b) as [ab|ab|ab].
    - rev_eq.
      destruct (ord b <?> ord c) as [bc|bc|bc]...
    - rev_lt.
      destruct (ord b <?> ord c) as [bc|bc|bc]...
      + rewrite (merge0_lt a c); sauto.
    - rev_gt.
      destruct (ord b <?> ord c) as [bc|bc|bc]...
      + rewrite (merge0_gt a c); sauto.
  Qed.

  Definition Last := option LogEntry.

  Definition merge (a b : Last) : Last :=
    match a, b with
    | None, None => None
    | Some a, None => Some a
    | None, Some b => Some b
    | Some a, Some b => Some (merge0 a b)
    end.

  Definition proper_pair_l a b :=
    match a, b with
    | Some a, Some b => proper_pair a b
    | _, _ => True
    end.

  Lemma merge_symm a b :
    proper_pair_l a b ->
    merge a b = merge b a.
  Proof.
    sauto unfold: merge, proper_pair_l use:merge0_symm.
  Qed.

  Lemma merge_refl a :
    merge a a = a.
  Proof.
    sauto unfold: merge use:merge0_refl.
  Qed.

  Lemma merge_assoc a b c :
    proper_pair_l a b ->
    proper_pair_l b c ->
    proper_pair_l a c ->
    merge (merge a b) c = merge a (merge b c).
  Proof.
    sauto unfold: merge, proper_pair_l use:merge0_assoc.
  Qed.

  Definition merge_l (l : list LogEntry) : Last :=
    let f acc elem := merge acc (Some elem) in
    fold_left f l None.

  Definition proper_log (l : list LogEntry) : Prop := ForallPairs proper_pair l.
End merge.
