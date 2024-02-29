import Entity "mo:CanDB/Entity";
import CanDB "mo:CanDB/CanDB";
import RBT "mo:stable-rbtree/StableRBTree";
import Text "mo:base/Text";
import Debug "mo:base/Debug";

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

  func replaceAttribute(db: CanDB.DB, options: { sk: Entity.SK; key: Entity.AttributeKey; value: Entity.AttributeValue })
      : async* ?Entity.Entity
  {
      CanDB.update(db, { sk = options.sk; updateAttributeMapFunction = func(old: ?Entity.AttributeMap): Entity.AttributeMap {
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