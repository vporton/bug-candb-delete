import CanDBIndex "canister:CanDBIndex";
import Text "mo:base/Text";
import CanDBPartition "../CanDBPartition";
import Principal "mo:base/Principal";
import Debug "mo:base/Debug";
import Entity "mo:CanDB/Entity";
import BT "mo:btree/BTree";
import RBT "mo:stable-rbtree/StableRBTree";

actor {
  public shared func main(): async () {
    let updateAttributeMapFunction = func(old: ?Entity.AttributeMap): Entity.AttributeMap {
      let map = switch (old) {
        case (?old) { old };
        case null { RBT.init() };
      };
      RBT.put(map, Text.compare, "i", #bool(false));
    };
    var data = BT.init<Entity.SK, Entity.AttributeMap>(?32);
    ignore BT.update(data, Text.compare, "a", updateAttributeMapFunction);
    Debug.print(debug_show(BT.delete(data, Text.compare, "a"))); // should print `?(#node(#B, #leaf, ("i", ?(#bool(false))), #leaf))`
  };
};
