import CanDBIndex "canister:CanDBIndex";
import CanDBPartition "../CanDBPartition";
import Principal "mo:base/Principal";

actor {
  public shared func main(): async () {
    await CanDBIndex.init([]);
    let can = await CanDBIndex.getCanistersByPK("main");
    let part: CanDBPartition.CanDBPartition = actor (can[0]);
    await part.putAttribute({key = "i"; sk = "i/2"; value = #tuple([#int(0), #bool(false), #int(+3), #text("slnzg-gc3pt-atp5r-qaa7q-3az2g-fcgb7-qf27s-khvrd-oo57m-23unl-dae"), #float(0.000000), #text("en"), #text("eee"), #text("")])});
    await part.delete({sk = "i/2"});
  };
};
