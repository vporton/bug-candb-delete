import Array "mo:base/Array";
import CA "mo:CanDB/CanisterActions";
import Entity "mo:CanDB/Entity";
import CanDB "mo:CanDB/CanDB";
import E "mo:CanDB/Entity";
import RBT "mo:stable-rbtree/StableRBTree";
import Principal "mo:base/Principal";
import Bool "mo:base/Bool";
import Debug "mo:base/Debug";
import Text "mo:base/Text";
import Buffer "mo:base/Buffer";
import Iter "mo:base/Iter";
import Nat "mo:base/Nat";

shared actor class CanDBPartition(options: {
  partitionKey: Text;
  scalingOptions: CanDB.ScalingOptions;
  owners: ?[Principal];
}) = this {
  stable var owners = switch (options.owners) {
    case (?p) { p };
    case _ { [] };
  };

  stable let db = CanDB.init({
    pk = options.partitionKey;
    scalingOptions = options.scalingOptions;
    btreeOrder = null;
  });

  public query func skExists(sk: Text): async Bool { 
    CanDB.skExists(db, sk);
  };

  public shared({caller}) func delete(options: CanDB.DeleteOptions): async () {
    CanDB.delete(db, options);
  };

  func replaceAttribute(db: CanDB.DB, options: { sk: E.SK; key: E.AttributeKey; value: E.AttributeValue })
      : async* ?E.Entity
  {
      CanDB.update(db, { sk = options.sk; updateAttributeMapFunction = func(old: ?E.AttributeMap): E.AttributeMap {
          let map = switch (old) {
              case (?old) { old };
              case null { RBT.init() };
          };
          RBT.put(map, Text.compare, options.key, options.value);
      }});
  };

  public shared({caller}) func putAttribute(options: { sk: Entity.SK; key: Entity.AttributeKey; value: Entity.AttributeValue }): async () {
    Debug.print("TRACE: putAttribute(" # debug_show(options) # ")");
    ignore await* replaceAttribute(db, options);
  };
}