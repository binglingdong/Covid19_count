for $itemID in distinct-values(doc("purchaseorders.xml")//item/partid)
let $price := doc("purchaseorders.xml")//item[partid = $itemID]/price
let $items := doc("purchaseorders.xml")//item[partid = $itemID]
order by $itemID
return <totalcost partid="{$itemID}">
{sum($items/quantity) * distinct-values($price)}
</totalcost>