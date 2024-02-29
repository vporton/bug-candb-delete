import CanDBIndex "canister:CanDBIndex";
import CanDBPartition "../CanDBPartition";
import Principal "mo:base/Principal";

actor {
  public shared func main(): async () {
    await CanDBIndex.init([]);
    let can = await CanDBIndex.getCanistersByPK("main");
    let part: CanDBPartition.CanDBPartition = actor (can[0]);
    await part.putAttribute({key = "i"; sk = "i/0"; value = #tuple([#int(0), #bool(true), #int(+3), #text("ruuoz-anyad-jumcs-huq7s-3eh7h-ja6j2-cmp2n-elv23-tghui-mve6f-xqe"), #float(0.000000), #text("en"), #text("The homepage"), #text("")])});
    await part.putAttribute({key = "i"; sk = "i/1"; value = #tuple([#int(0), #bool(false), #int(+3), #text("slnzg-gc3pt-atp5r-qaa7q-3az2g-fcgb7-qf27s-khvrd-oo57m-23unl-dae"), #float(0.000000), #text("en"), #text("qqq"), #text("")])});
    await part.putAttribute({key = "st"; sk = "i/0"; value = #tuple([#int(-1), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(0), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+1), #int(-1)])});
    await part.putAttribute({key = "srt"; sk = "i/1"; value = #tuple([#int(-1), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+2), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+3), #int(-1)])});
    await part.putAttribute({key = "sv"; sk = "i/0"; value = #tuple([#int(-1), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+4), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+5), #int(-1)])});
    await part.putAttribute({key = "srv"; sk = "i/1"; value = #tuple([#int(-1), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+6), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+7), #int(-1)])});
    await part.putAttribute({key = "i"; sk = "i/2"; value = #tuple([#int(0), #bool(false), #int(+3), #text("slnzg-gc3pt-atp5r-qaa7q-3az2g-fcgb7-qf27s-khvrd-oo57m-23unl-dae"), #float(0.000000), #text("en"), #text("eee"), #text("")])});
    await part.putAttribute({key = "st"; sk = "i/0"; value = #tuple([#int(-1), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(0), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+1), #int(-1)])});
    await part.putAttribute({key = "srt"; sk = "i/2"; value = #tuple([#int(-1), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+8), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+9), #int(-1)])});
    await part.putAttribute({key = "sv"; sk = "i/0"; value = #tuple([#int(-1), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+4), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+5), #int(-1)])});
    await part.putAttribute({key = "srv"; sk = "i/2"; value = #tuple([#int(-1), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+10), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+11), #int(-1)])});
    await part.putAttribute({key = "i"; sk = "i/3"; value = #tuple([#int(0), #bool(false), #int(+3), #text("slnzg-gc3pt-atp5r-qaa7q-3az2g-fcgb7-qf27s-khvrd-oo57m-23unl-dae"), #float(0.000000), #text("en"), #text("zzz"), #text("")])});
    await part.putAttribute({key = "st"; sk = "i/0"; value = #tuple([#int(-1), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(0), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+1), #int(-1)])});
    await part.putAttribute({key = "srt"; sk = "i/3"; value = #tuple([#int(-1), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+12), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+13), #int(-1)])});
    await part.putAttribute({key = "sv"; sk = "i/0"; value = #tuple([#int(-1), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+4), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+5), #int(-1)])});
    await part.putAttribute({key = "srv"; sk = "i/3"; value = #tuple([#int(-1), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+14), #text("ahw5u-keaaa-aaaaa-qaaha-cai"), #int(+15), #int(-1)])});
    await part.delete({sk = "i/2"});
  };
};
