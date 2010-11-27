library(RODBC)
fisidbchann<-odbcConnectAccess("C:\\Users\\Eli Swanson\\Documents\\My Dropbox\\School\\Data, analyses\\Growth\\2010\\accessfisi_be")
#sqlDrop(fisidbchann, "grow", errors=F)
growQry<-paste("SELECT grow.* FROM grow")
grow<-sqlQuery(fisidbchann, growQry, errors = TRUE)



###################BINALPHA CALCULATION##########################

###This following sequence creates a table and pulls into into R as a data
#frame of every hyena in tblHyenas, along with every one of their female
#ancestors in additional columns and a final column giving their oldest
#known ancestor

sqlDrop(fisidbchann,"momsAndGrandmas", errors=F)
momsAndGrandmasQry<-paste("SELECT tH.ID, tH.Mom, tH1.Mom AS Grandma
FROM tblHyenas AS tH LEFT JOIN tblHyenas AS tH1 ON tH.Mom = tH1.ID
ORDER BY tH1.Mom,tH.Mom,tH.ID")
momsAndGrandmas<-sqlQuery(fisidbchann, momsAndGrandmasQry, errors = TRUE)
sqlSave(fisidbchann, momsAndGrandmas, fast=F)


sqlDrop(fisidbchann, "gGrandmas",errors=F)
gGrandmasQry<-paste("SELECT mAG.ID, mAG.Mom, mAG.Grandma, tH.Mom AS gGrandma
FROM momsAndGrandmas AS mAG LEFT JOIN tblHyenas AS tH ON mAG.Grandma = tH.ID
ORDER BY tH.Mom,mAG.Grandma,mAG.Mom,mAG.ID")
gGrandmas<-sqlQuery(fisidbchann, gGrandmasQry, errors = TRUE)
sqlSave(fisidbchann,gGrandmas,fast=F)


sqlDrop(fisidbchann, "ggGrandmas",errors=F)
ggGrandmasQry<-paste("SELECT gG.ID, gG.Mom, gG.Grandma, gG.gGrandma, tH.Mom AS ggGrandma
FROM gGrandmas AS gG LEFT JOIN tblHyenas AS tH ON gG.gGrandma = tH.ID
ORDER BY tH.Mom,gG.gGrandma,gG.Grandma, gG.Mom, gG.ID")
ggGrandmas<-sqlQuery(fisidbchann, ggGrandmasQry, errors = TRUE)
sqlSave(fisidbchann,ggGrandmas,fast=F)


sqlDrop(fisidbchann, "gggGrandmas",errors=F)
gggGrandmasQry<-paste("SELECT ggG.ID, ggG.Mom, ggG.Grandma, ggG.gGrandma, ggG.ggGrandma, tH.Mom AS gggGrandma
FROM ggGrandmas AS ggG LEFT JOIN tblHyenas AS tH ON ggG.ggGrandma = tH.ID
ORDER BY tH.Mom, ggG.ggGrandma, ggG.gGrandma, ggG.Grandma, ggG.Mom, ggG.ID")
gggGrandmas<-sqlQuery(fisidbchann, gggGrandmasQry, errors = TRUE)
sqlSave(fisidbchann,gggGrandmas,fast=F)


sqlDrop(fisidbchann, "ggggGrandmas",errors=F)
ggggGrandmasQry<-paste("SELECT gggG.ID, gggG.Mom, gggG.Grandma, gggG.gGrandma, gggG.ggGrandma, gggG.gggGrandma, tH.Mom AS ggggGrandma
FROM gggGrandmas AS gggG LEFT JOIN tblHyenas AS tH ON gggG.gggGrandma = tH.ID
ORDER BY tH.Mom, gggG.gggGrandma, gggG.ggGrandma, gggG.gGrandma, gggG.Grandma, gggG.Mom, gggG.ID")
ggggGrandmas<-sqlQuery(fisidbchann, ggggGrandmasQry, errors = TRUE)
sqlSave(fisidbchann,ggggGrandmas,fast=F)


#############Need some kind of union qry (or set of union queries) to pull out the oldest known ancestor for each female

sqlDrop(fisidbchann, "oldestKnownAncestor", errors=F)
oldestKnownAncestorQry<-paste("SELECT gA.ID, gA.ggggGrandma AS oldestKnownAncestor FROM ggggGrandmas AS gA WHERE gA.ggggGrandma IS NOT NULL
 UNION SELECT gA.ID, gA.gggGrandma AS oldestKnownAncestor from ggggGrandmas AS gA where gA.ggggGrandma IS NULL AND gA.gggGrandma IS NOT NULL
 UNION SELECT gA.ID, gA.ggGrandma AS oldestKnownAncestor from ggggGrandmas AS gA where gA.ggggGrandma IS NULL AND gA.gggGrandma IS NULL AND gA.ggGrandma IS NOT NULL
 UNION SELECT gA.ID, gA.gGrandma AS oldestKnownAncestor from ggggGrandmas AS gA where gA.ggggGrandma IS NULL AND gA.gggGrandma IS NULL AND gA.ggGrandma IS NULL AND gA.gGrandma IS NOT NULL
 UNION SELECT gA.ID, gA.Grandma AS oldestKnownAncestor from ggggGrandmas AS gA where gA.ggggGrandma IS NULL AND gA.gggGrandma IS NULL AND gA.ggGrandma IS NULL AND gA.gGrandma IS NULL AND gA.Grandma IS NOT NULL
 UNION SELECT gA.ID, gA.Mom AS oldestKnownAncestor from ggggGrandmas AS gA where gA.ggggGrandma IS NULL AND gA.gggGrandma IS NULL AND gA.ggGrandma IS NULL AND gA.gGrandma IS NULL AND gA.Grandma IS NULL AND gA.Mom IS NOT NULL
 UNION SELECT gA.ID, gA.ID AS oldestKnownAncestor from ggggGrandmas AS gA where gA.ggggGrandma IS NULL AND gA.gggGrandma IS NULL AND gA.ggGrandma IS NULL AND gA.gGrandma IS NULL AND gA.Grandma IS NULL AND gA.Mom IS NULL ORDER BY oldestKnownAncestor")
oldestKnownAncestor<-sqlQuery(fisidbchann,oldestKnownAncestorQry, errors=T)



###End of moms qry


###Make a table with oldestknownancestors and a matriline grouping, figure out if i need to add more 1s here
sqlDrop(fisidbchann, "ancestorBinAlpha",errors=F)
ancestorBinAlpha<-data.frame(oldestKnownAncestor,"binalpha"=0)
ancestorBinAlpha$binalpha[ancestorBinAlpha$oldestKnownAncestor=="kb"]<-1
sqlSave(fisidbchann,ancestorBinAlpha,fast=F, rownames=F)

sqlDrop(fisidbchann, "growBinAlpha", errors=F)
growBinAlphaQry<-paste("SELECT g.*, bA.oldestKnownAncestor, bA.binalpha
FROM grow AS g
LEFT JOIN ancestorBinAlpha AS bA ON g.Hyena=bA.ID
ORDER BY bA.oldestKnownAncestor, g.Hyena")
growBinAlpha<-sqlQuery(fisidbchann, growBinAlphaQry, errors=T)
sqlSave(fisidbchann, growBinAlpha, fast=F, rownames=F)

#################AGE CALCULATIONS#####################


#growAge will combine AgeMonths and EstimatedAgeMo, then in R I will manually add in recent values that I've updated.
#Union agemonths and EstimatedAgeMo where
growBinAlpha<-growBinAlpha[order(growBinAlpha$UID),]
growBinAlpha$EstimatedAgeMo<-as.numeric(growBinAlpha$EstimatedAgeMo)
growBinAlpha$Age<-ifelse(!is.na(growBinAlpha$AgeMonths),growBinAlpha$AgeMonths,growBinAlpha$EstimatedAgeMo)
grow1<-growBinAlpha

###Now enter ages for individuals that I calculated (in Estimated ages for Kay to enter from Eli.xls)
grow1$Age[grow1$Hyena=='scy' & grow1$DartingDate
