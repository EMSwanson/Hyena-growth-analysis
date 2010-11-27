
###Second shot at sorting - this creates a growth dataset from 
#the larger dataset of a mixture of ranks and stuff 
f<-function(x)
{
  d<-data.frame(NULL)
  nr1<-length(unique(x$Hyena))
  for(i in 1:nr1)
  {
    print(i)
    a<-x[x$Hyena==noquote(unique(x$Hyena))[i],]                                    
    nr2<-length(unique(a$DartingDate))
    for(j in 1:nr2)
    {
      b<-a[a$DartingDate==noquote(unique(a$DartingDate))[j],]
      c<-b[b$qryNumber==max(b$qryNumber),]
      d<-rbind(d,c)
    }
  }  
return(d)
}  






library(RODBC)
fisidbchann<-odbcConnectAccess("C:\\Users\\Eli Swanson\\Documents\\My Dropbox\\School\\Data, analyses\\Growth\\2010\\accessfisi_be")
#sqlTables(db)
#sqlTables(ch, tableType = "TABLE")
#sqlTables(ch, schema = "some pattern ")
#sqlTables(ch, tableName = "some pattern ")
tblDarting <- sqlFetch(fisidbchann, "tblDarting")
tblHyenas <- sqlFetch(fisidbchann, "tblHyenas")
names (tblDarting)
names(tblHyenas)

#DISTINCT 
#LIMIT
#CASE
#AS - dont really understand aliasing yet


###GrowOneQry grabs the darting data needed from table darting
#and left outer joins to table hyenas where we appropriate birthdata and mom
sqlDrop(fisidbchann, "growOne", errors=F)#Deletes the current version of the table from the db 
growOneQry<-paste("SELECT tD.Hyena, tD.DartingDate, tD.Sex, tH.Mom, tH.Birthdate,", 
"tD.Age, tD.AgeMonths, tD.Clan, tD.NatalOrImmigrant, tD.MaternalRank AS Rank,",
"tD.Mass, tD.ZygoToTopCrest, tD.ZygoToBackCrest, tD.SkullLength, tD.HeadCirc,", 
"tD.BodyLength, tD.NeckCirc, tD.Girth, tD.FrontFtLen, tD.LowerLegLength,", 
"tD.UpperLegLength, tD.ScapulaLength, tD.HtAtShoulder, tD.HindFtLength,",
"tD.EstimatedAgeMo, tD.AgeAtDartingDays, tD.AgeAtDartingNote, '1' AS qryNumber",
                  "FROM tblDarting AS tD",
                    "LEFT JOIN", 
                    "tblHyenas AS tH",
                  "ON tH.ID=tD.ID",
                  "WHERE tD.Sex <> 'u' AND tD.Sex IS NOT NULL",
                  "AND tD.DartingDate IS NOT NULL AND tD.Hyena IS NOT NULL",
                  "ORDER BY tD.Sex,tD.DartingDate")
growOne<-sqlQuery(fisidbchann, growOneQry, errors = TRUE)   #Runs the query
UID<-1:nrow(growOne)
growOne<-cbind(UID,growOne)
sqlSave(fisidbchann, growOne, fast=F, varTypes=c(DartingDate="DateTime",Birthdate="DateTime"),rownames=F)  #Creates the table in the db


###growTwo should add cub ranks for all animals that have maternal ranks but not their own ranks in tblFemaleRanksWithClanSplit2001
sqlDrop(fisidbchann, "growTwo", errors=F)
growTwoQry<-paste("SELECT gO.UID, gO.Hyena, gO.DartingDate, gO.Sex, gO.Mom, gO.Birthdate,
gO.Age, gO.AgeMonths, gO.Clan, gO.NatalOrImmigrant, fR.Rank AS Rank,
gO.Mass, gO.ZygoToTopCrest, gO.ZygoToBackCrest, gO.SkullLength, gO.HeadCirc,
gO.BodyLength, gO.NeckCirc, gO.Girth, gO.FrontFtLen, gO.LowerLegLength,
gO.UpperLegLength, gO.ScapulaLength, gO.HtAtShoulder, gO.HindFtLength,
gO.EstimatedAgeMo, gO.AgeAtDartingDays, gO.AgeAtDartingNote, '3' AS qryNumber
FROM growOne AS gO LEFT JOIN tblFemaleRanksWithClanSplit2001 as fR
ON gO.Mom=fR.Hyena
WHERE fR.Year=Year([gO].[DartingDate])")
growTwo<-sqlQuery(fisidbchann, growTwoQry, errors = TRUE)   #Runs the query
sqlSave(fisidbchann, growTwo, fast=F, varTypes=c(DartingDate="DateTime",Birthdate="DateTime"),rownames=F)


##Then outer join the result to the Growth dataset, something like what follows, though the following code is incomplete
#growThree is going to add female ranks for adult females that have their own ranks
sqlDrop(fisidbchann, "growThree", errors=F)
growThreeQry<-paste("SELECT gO.UID, gO.Hyena, gO.DartingDate, gO.Sex, gO.Mom, gO.Birthdate,
gO.Age, gO.AgeMonths, gO.Clan, gO.NatalOrImmigrant, fR.Rank AS Rank,         
gO.Mass, gO.ZygoToTopCrest, gO.ZygoToBackCrest, gO.SkullLength, gO.HeadCirc,
gO.BodyLength, gO.NeckCirc, gO.Girth, gO.FrontFtLen, gO.LowerLegLength,
gO.UpperLegLength, gO.ScapulaLength, gO.HtAtShoulder, gO.HindFtLength,
gO.EstimatedAgeMo, gO.AgeAtDartingDays, gO.AgeAtDartingNote, '2' AS qryNumber
FROM growOne AS gO LEFT JOIN tblFemaleRanksWithClanSplit2001 AS fR 
ON gO.Hyena=fR.Hyena 
WHERE fR.Year=Year([gO].[DartingDate])")
growThree<-sqlQuery(fisidbchann, growThreeQry, errors = TRUE)   #Runs the query
sqlSave(fisidbchann, growThree, fast=F, varTypes=c(DartingDate="DateTime",Birthdate="DateTime"),rownames=F)


###Union query to union animals with no ranks, females with their adult ranks, and animals that only have maternal ranks
###Still need to get this union query working. I think I need to use all the columns from growOne in growTwo and growThree, then just union select all
sqlDrop(fisidbchann, "growWithRanks", errors=F)
growWithRanksQry<-paste("
SELECT * FROM growTwo AS gT 
UNION
SELECT * FROM growOne AS gO 
UNION 
SELECT * FROM growThree AS gTh
")
growWithRanks<-sqlQuery(fisidbchann, growWithRanksQry, errors=T)
sqlSave(fisidbchann, growWithRanks, fast=F, varTypes=c(DartingDate="DateTime",Birthdate="DateTime"))


###7 individuals have duplicate records where they have different ranks
#Run these if I want to reassess the grow table
#grow<-f(growWithRanks)
#sqlSave(fisidbchann, grow, fast=F, varTypes=c(DartingDate="DateTime",Birthdate="DateTime"))
#grow is now a table in the access database



 