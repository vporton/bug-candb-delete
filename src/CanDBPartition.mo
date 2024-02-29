import Array "mo:base/Array";
import CA "mo:CanDB/CanisterActions";
import Entity "mo:CanDB/Entity";
import CanDB "mo:CanDB/CanDB";
import Multi "mo:CanDBMulti/Multi";
import RBT "mo:stable-rbtree/StableRBTree";
import Principal "mo:base/Principal";
import Bool "mo:base/Bool";
import Debug "mo:base/Debug";
import Text "mo:base/Text";
import Buffer "mo:base/Buffer";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";
// import lib "../backend/lib";

shared actor class CanDBPartition(options: {
  partitionKey: Text;
  scalingOptions: CanDB.ScalingOptions;
  owners: ?[Principal];
}) = this {
  stable var owners = switch (options.owners) {
    case (?p) { p };
    case _ { [] };
  };

  /// @required (may wrap, but must be present in some form in the canister)
  stable let db = CanDB.init({
    pk = options.partitionKey;
    scalingOptions = options.scalingOptions;
    btreeOrder = null;
  });

  public shared({caller}) func setOwners(_owners: [Principal]): async () {
    owners := _owners;
  };

  public query func getOwners(): async [Principal] { owners };

  /// @recommended (not required) public API
  public query func getPK(): async Text { db.pk };

  /// @required public API (Do not delete or change)
  public query func skExists(sk: Text): async Bool { 
    CanDB.skExists(db, sk);
  };

  public query func get(options: CanDB.GetOptions): async ?Entity.Entity { 
    CanDB.get(db, options);
  };

  public shared({caller}) func put(options: CanDB.PutOptions): async () {
    await* CanDB.put(db, options);
  };

  public shared({caller}) func delete(options: CanDB.DeleteOptions): async () {
    Debug.print("TRACE: delete(" # debug_show(options) # ")");

    CanDB.delete(db, options);
  };

  public query func scan(options: CanDB.ScanOptions): async CanDB.ScanResult {
    CanDB.scan(db, options);
  };

  /// @required public API (Do not delete or change)
  public shared({caller}) func transferCycles(): async () {
    return await CA.transferCycles(caller);
  };

  func _getAttribute(options: CanDB.GetOptions, subkey: Text): ?Entity.AttributeValue {
    let all = CanDB.get(db, options);
    do ? { RBT.get(all!.attributes, Text.compare, subkey)! };
  };

  public query func getAttribute(options: CanDB.GetOptions, subkey: Text): async ?Entity.AttributeValue {
    _getAttribute(options, subkey);
  };

  // Application-specific code //

  // public query func getItem(itemId: Nat): async ?lib.Item {
  //   let data = _getAttribute({sk = "i/" # Nat.toText(itemId)}, "i");
  //   do ? { lib.deserializeItem(data!) };
  // };

  // public query func getStreams(itemId: Nat, kind: Text): async ?lib.Streams {
  //   // TODO: Duplicate code
  //   let data = _getAttribute({sk = "i/" # Nat.toText(itemId)}, "s" # kind);
  //   do ? { lib.deserializeStreams(data!) };
  // };

  // CanDBMulti //

  public shared({caller}) func putAttribute(options: { sk: Entity.SK; key: Entity.AttributeKey; value: Entity.AttributeValue }): async () {
    Debug.print("TRACE: putAttribute(" # debug_show(options) # ")");
    ignore await* Multi.replaceAttribute(db, options);
  };

  public shared({caller}) func putExisting(options: CanDB.PutOptions): async Bool {
    Debug.print("TRACE: putExisting(" # debug_show(options) # ")");
    await* Multi.putExisting(db, options);
  };

  public shared({caller}) func putExistingAttribute(options: { sk: Entity.SK; key: Entity.AttributeKey; value: Entity.AttributeValue })
    : async Bool
  {
    Debug.print("TRACE: putExistingAttribute(" # debug_show(options) # ")");
    await* Multi.putExistingAttribute(db, options);
  };
}