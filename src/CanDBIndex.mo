import Cycles "mo:base/ExperimentalCycles";
import Debug "mo:base/Debug";
import CA "mo:CanDB/CanisterActions";
import Utils "mo:CanDB/Utils";
import CanisterMap "mo:CanDB/CanisterMap";
import Buffer "mo:stable-buffer/StableBuffer";
import CanDBPartition "CanDBPartition";
import Principal "mo:base/Principal";

shared({caller = initialOwner}) actor class CanDBIndex() = this {
  stable var owners: [Principal] = [initialOwner];

  stable var initialized: Bool = false;

  public shared({caller}) func init(_owners: [Principal]): async () {
    if (initialized) {
      return;
    };

    owners := _owners;
    ignore await* createStorageCanister("main", [caller]);

    initialized := true;
  };

  func ownersOrSelf(): [Principal] {
    let buf = Buffer.fromArray<Principal>(owners);
    Buffer.add(buf, Principal.fromActor(this));
    Buffer.toArray(buf);
  };

  let maxSize = #heapSize(500_000_000);

  stable var pkToCanisterMap = CanisterMap.init();

  public shared query({caller}) func getCanistersByPK(pk: Text): async [Text] {
    getCanisterIdsIfExists(pk);
  };

  func getCanisterIdsIfExists(pk: Text): [Text] {
    switch(CanisterMap.get(pkToCanisterMap, pk)) {
      case null { [] };
      case (?canisterIdsBuffer) { Buffer.toArray(canisterIdsBuffer) } 
    }
  };

  public shared({caller}) func autoScaleCanister(pk: Text): async Text {
    if (Utils.callingCanisterOwnsPK(caller, pkToCanisterMap, pk)) {
      await* createStorageCanister(pk, ownersOrSelf());
    } else {
      Debug.trap("error, called by non-controller=" # debug_show(caller));
    };
  };

  func createStorageCanister(pk: Text, controllers: [Principal]): async* Text {
    Debug.print("creating new storage canister with pk=" # pk);
    // Pre-load 300 billion cycles for the creation of a new storage canister
    // Note that canister creation costs 100 billion cycles, meaning there are 200 billion
    // left over for the new canister when it is created
    Cycles.add(210_000_000_000); // TODO: Choose the number.
    let newStorageCanister = await CanDBPartition.CanDBPartition({
      partitionKey = pk;
      scalingOptions = {
        autoScalingHook = autoScaleCanister;
        sizeLimit = maxSize;
      };
    });
    let newStorageCanisterPrincipal = Principal.fromActor(newStorageCanister);
    await CA.updateCanisterSettings({
      canisterId = newStorageCanisterPrincipal;
      settings = {
        controllers = ?controllers;
        compute_allocation = ?0;
        memory_allocation = ?0;
        freezing_threshold = ?2592000;
      }
    });

    let newStorageCanisterId = Principal.toText(newStorageCanisterPrincipal);
    pkToCanisterMap := CanisterMap.add(pkToCanisterMap, pk, newStorageCanisterId);

    Debug.print("new storage canisterId=" # newStorageCanisterId);
    newStorageCanisterId;
  };
}