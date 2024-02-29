import CanDBIndex "canister:CanDBIndex";
import CanDBPartition "../CanDBPartition";
import Principal "mo:base/Principal";

actor {
  public shared func main(): async () {
    await CanDBIndex.init([]);
    let can = await CanDBIndex.getCanistersByPK("main");
    let part: CanDBPartition.CanDBPartition = actor (can[0]);
    await part.putAttribute({key = "i"; sk = "a"; value = #bool(false)});
    await part.delete({sk = "a"});
  };
};
