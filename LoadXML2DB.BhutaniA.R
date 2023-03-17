#FINAL SUBMISSION

#Importing necessary libraries

library(XML)

library(RSQLite)

library(tidyverse)

#Retriving the current directory's path and storing in variable filePath
filePath = getwd()
dbfile = "ArticleDB.sqlite"

#Creating a connection with the ArticleDB database and storing it in dbcon

dbcon <- dbConnect(RSQLite::SQLite(), paste0(filePath, dbfile))

#Creating the schema to store the retrieved data from the XML file into the SQLite Database.

#Following tables are created to make sure the schema is as normalized as possible:
  
  #1. CiteMed: One to Many relationship with Article
  #2. Language: One to Many relationship with Article
  #3. ISSN: One to Many relationship with Article 
  #4. ISOAbbr: One to Many relationship with Article
  #5. Author_Collab: Created to resolve the many to many relationship b/w Article and Author
  #6. Article: Main table that contains llthe info about published articles.
  #7. Journal: One to many relationship with Article
  #8. Author: Many to many relationship with Article

dbExecute(conn=dbcon, statement='DROP TABLE IF EXISTS CiteMed')

dbExecute(conn=dbcon, statement='CREATE TABLE CiteMed(CitedMed Text, CiteMed_idx Number, Primary key(CiteMed_idx))')

dbExecute(conn=dbcon, statement='DROP TABLE IF EXISTS Language')

dbExecute(conn=dbcon, statement='CREATE TABLE Language(Lang Text, Lang_idx Number, Primary Key(Lang_idx))')

dbExecute(conn=dbcon, statement='DROP TABLE IF EXISTS ISSN')

dbExecute(conn=dbcon, statement='CREATE TABLE ISSN(Type Text, ISSN_idx Number,  Primary key(ISSN_idx))')

dbExecute(conn=dbcon, statement='DROP TABLE IF EXISTS ISOAbbr')

dbExecute(conn=dbcon, statement='CREATE TABLE ISOAbbr(ISOAbbr Text, ISOAbbr_idx Number, Primary key(ISOAbbr_idx))')

dbExecute(conn=dbcon, statement='DROP TABLE IF EXISTS Journal')

dbExecute(conn=dbcon, statement='CREATE TABLE Journal(ISSN_Num Text, Journal_Title Text, Journal_Id Number,  Primary key(Journal_Id))')

dbExecute(conn=dbcon, statement='DROP TABLE IF EXISTS Author')

dbExecute(conn=dbcon, statement='CREATE TABLE Author(LastName Text, Initial Text, Author_Id Number,  Primary key(Author_Id))')

dbExecute(conn=dbcon, statement='DROP TABLE IF EXISTS Article')

dbExecute(conn=dbcon, statement='CREATE TABLE Article(Article_Id Number, Type Text, PMID Number, Volume Number, Issue Number, Year Number, Month Number, Day Number, Art_Title Text, Lang_idx Number, CiteMed_idx Number, ISSN_idx Number, ISOAbbr_idx Number, Journal_Id Number,    
                                Primary key(Article_Id), FOREIGN KEY(ISSN_idx) REFERENCES ISSN (ISSN_idx), FOREIGN KEY(CiteMed_idx) REFERENCES CiteMed (CiteMed_idx), 
                                FOREIGN KEY(Lang_idx) REFERENCES Language (Lang_idx), FOREIGN KEY(ISOAbbr_idx) REFERENCES ISOAbbr (ISOAbbr_idx), 
                                FOREIGN KEY(Journal_Id) REFERENCES Journal (Journal_Id))')

dbExecute(conn=dbcon, statement='DROP TABLE IF EXISTS Author_Collab')

dbExecute(conn=dbcon, statement='CREATE TABLE Author_Collab(Article_Id Number, Author_Id Number, FOREIGN KEY(Author_Id) REFERENCES Author (Author_Id), 
                                 FOREIGN KEY(Article_Id) REFERENCES Article (Article_Id))')




# Creating an xml tree object for the given xml file
xml.dom <- xmlParse("pubmed-tfm-xml/pubmed22n0001-tf.xml")

root <- xmlRoot(xml.dom)

n <- xmlSize(root)

row <- root[[1]]

#Creating necessary vectors to store the retrieved information from the XML document.

ISSN_Num <- c()
Volume <- c()
Issue <- c()
Year <- c()
Month <- c()
Day <- c()
Journal_Title <- c()
ISOAbbr <- c()
Art_Title <- c()
Lang <- c()
LastName <- c()
ForeName <- c()
Initial <- c()
Auth_Jour <- c()
ISSN_type <- c()
Medium <- c()
CitedMed <- c()
Type <- c()
PMID <- c()
Auth_Art_Id <- c()
Auth_Art_Title <- c()


#Interating over all of the root elements in the XML doc. A total of ~30k.

for(i in 1:n){
  
  row <- root[[i]]
  
  #Storing the subparts of an element of root into different variables.
  Journal_info <- row[[1]][[1]]
  Language_info <- row[[1]][[2]]
  Article_title <- row[[1]][[3]]
  Author_list <- row[[1]][[4]]
  
  #Storing the required information from the above created sub-parts and appending into the vector.
  ISSN_Num <- append(ISSN_Num, xmlValue(Journal_info[[1]]))
  Volume <- append(Volume, xmlValue(Journal_info[[2]][[1]]))
  Issue <- append(Issue, xmlValue(Journal_info[[2]][[2]]))
  
  #Since year contains a lot of anomolous data. We consider only the first year from the whole string which is a substr of length 4.
  year <- xmlValue(Journal_info[[2]][[3]][[1]])
  Year <- append(Year, substring(year, 1, 4))
  
  #Month data is encoded as integers ranging from 1-12 to save space.
  month <- xmlValue(Journal_info[[2]][[3]][[2]])
  
  if (is.na(month)){
    month <- NA
  }
  else if (month == "Summer"){
    month <- "05"
  }
  else if(month == "Spring"){
    month <- "03"
  }
  else if(month == "winter"){
    month <- "12"
  }
  else if(month == "Fall"){
    month <- "09"
  }
  else if(month == "Jan"){
    month <- "01"
  }
  else if(month == "Feb"){
    month <- "02"
  }
  else if(month == "Mar"){
    month <- "03"
  }
  else if(month == "Apr"){
    month <- "04"
  }
  else if(month == "May"){
    month <- "05"
  }
  else if(month == "Jun"){
    month <- "06"
  }
  else if(month == "Jul"){
    month <- "07"
  }
  else if(month == "Aug"){
    month <- "08"
  }
  else if(month == "Sep"){
    month <- "09"
  }
  else if(month == "Oct"){
    month <- "10"
  }
  else if(month == "Nov"){
    month <- "11"
  }
  else if(month == "Dec"){
    month <- "12"
  }
  else{
    month <- NA
  }
  
  Month <- append(Month, month)
  
  
  Day <- append(Day, xmlValue(Journal_info[[2]][[3]][[3]]))
  Journal_Title <- append(Journal_Title, xmlValue(Journal_info[[3]]))
  ISOAbbr <- append(ISOAbbr, xmlValue(Journal_info[[4]]))
  Art_Title <- append(Art_Title, xmlValue(Article_title))
  Lang <- append(Lang, xmlValue(Language_info))
  
  medium <- xmlGetAttr(Journal_info[[2]], "CitedMedium", default = NULL) 
  
  
  #Handling Null values since they can be used to potentially join the tables or identify rows in a table uniquely.
  
  if(is.null(medium)){
    CitedMed <- append(CitedMed, "NULL")
  }
  else{
    CitedMed <- append(CitedMed, medium)
  }
  
  ISSN_type <- xmlGetAttr(Journal_info[[1]], "IssnType", default = NULL) 
  
  if(is.null(ISSN_type)){
    Type <- append(Type, "NULL")
  }
  else{
    Type <- append(Type, ISSN_type)
  }
  
  Id <- xmlGetAttr(root[[i]], "PMID", default = NULL)
  
  if(is.null(Id)){
    Id <- "NULL"
    PMID <- append(PMID, Id)
  }
  else{
    PMID <- append(PMID, Id)
  }
  
  #For each article iterating over all the authors and appending them to the Last name and Initial vectors.
  #Here, Forename is not retrieved since it is same as the initials data and both last name and 
  #initials are enough to identify an author uniquely
  for(j in 1:xmlSize(Author_list)){
    LastName<- append(LastName, xmlValue(Author_list[[j]][[1]]))
    Initial <- append(Initial, xmlValue(Author_list[[j]][[2]]))
    #Storing the PMID, Article title for each author separately as it will help in resolving the Many-to-Many relation between Author and Article
    Auth_Art_Id <- append(Auth_Art_Id, Id)
    Auth_Art_Title <- append(Auth_Art_Title, xmlValue(Article_title))
  }
   
}




#Article_df <- data.frame(Article_Id = integer(),
#                        ISSN_Type = character(),
#                        *ISSN_Num = character(),
#                        Cite_Medium = character(),
#                        *Volume = integer(),
#                        *Issue = integer(),
#                        *Pub_Date = integer(),
#                        *Pub_Month = integer(),
#                        *Pub_Year = integer(),
#                        *Title = character(),
#                        *Language = character(),
#                         Art_Title = character())

Article_df <- data.frame(Type, PMID, CitedMed, Volume, Issue, Year, Month, Day, Art_Title, ISOAbbr, Lang, Journal_Title, ISSN_Num)

#Journaldf <- data.frame(Journal_Id = integer(),
#                        Journal_Title = character())

Journal_df <- data.frame(ISSN_Num, Journal_Title)


#Author_Collab_df <- data.frame(Author_Art_Id = integer(),
#                       Author_LName = character(),
#                       Author_Initial = character(),
#                       Author_Art_Title = character())

Author_Collab_df <- data.frame(LastName, Initial, Auth_Art_Id, Auth_Art_Title)

Author_Collab_df <- Author_Collab_df %>% rename(PMID = Auth_Art_Id, Art_Title = Auth_Art_Title)

#Author_df <- data.frame(Author_Id = integer(),
#                       Author_LName = character(),
#                       Author_Initial = character())

Author_df <- data.frame(LastName, Initial)

#ISSNdf <- data.frame(ISSN_Type = character(),
#                     ISSN_Id = integer())

ISSN_df <- data.frame(Type)


#CiteMeddf <- data.frame(Medium_Type = character(),
#                        Medium_Id = integer())

CiteMed_df <- data.frame(CitedMed)

#Languagedf <- data.frame(Language = character(),
#                         Language_Id = integer())


Language_df <- data.frame(Lang)

#ISSOAbbrdf <- data.frame(Abbrtype = character(),
#                         Abbr_Id = integer())

ISOAbbr_df <- data.frame(ISOAbbr)


#Creating surrogate Primary keysfor the tables below and removing duplicate rows from each table.

Language_df = unique(Language_df)
Language_df["Lang_idx"] <- NA
Language_df['Lang_idx'] = 1:nrow(Language_df)

ISSN_df = unique(ISSN_df)
ISSN_df["ISSN_idx"] <- NA
ISSN_df['ISSN_idx'] = 1:nrow(ISSN_df)


CiteMed_df = unique(CiteMed_df)
CiteMed_df["CiteMed_idx"] <- NA
CiteMed_df['CiteMed_idx'] = 1:nrow(CiteMed_df)

ISOAbbr_df = unique(ISOAbbr_df)
ISOAbbr_df['ISOAbbr_idx'] <- NA
ISOAbbr_df['ISOAbbr_idx'] = 1:nrow(ISOAbbr_df)


Author_df = unique(Author_df)
Author_df['Author_Id'] <- NA
Author_df['Author_Id'] = 1:nrow(Author_df)

Journal_df  = unique(Journal_df)
Journal_df['Journal_Id'] <- NA
Journal_df['Journal_Id'] = 1:nrow(Journal_df)

Article_df  = unique(Article_df)
Article_df['Article_Id'] <- NA
Article_df['Article_Id'] = 1:nrow(Article_df)

#Joining tables together to satisfy the relations between them and create foreign key data.

library('plyr')
Author_Collab_df <- join(Author_Collab_df, Author_df, type = "inner")
Author_Collab_df <- subset(Author_Collab_df, select = -c(LastName, Initial))

Author_Collab_df <- join(Author_Collab_df, Article_df, type = "inner")
Author_Collab_df <- subset(Author_Collab_df, select = c(Author_Id, Article_Id))


Article_df <- join(Article_df, Language_df, type = "inner")
Article_df <- join(Article_df, CiteMed_df, type = "inner")
Article_df <- join(Article_df, ISSN_df, type = "inner")
Article_df <- join(Article_df, ISOAbbr_df, type = "inner")
Article_df <- join(Article_df, Journal_df, type = "inner")

Article_df <- subset(Article_df, select = -c(Lang, CitedMed, ISOAbbr, Type, Journal_Title, ISSN_Num))


#Writing all the info from Data Frames into SQLite tables using dbWriteTable.

dbWriteTable(dbcon, "CiteMed", CiteMed_df, overwrite = F, append = T, row.names=F)
dbWriteTable(dbcon, "Language", Language_df, overwrite = F, append = T, row.names=F)
dbWriteTable(dbcon, "ISSN", ISSN_df, overwrite = F, append = T, row.names=F)
dbWriteTable(dbcon, "ISOAbbr", ISOAbbr_df, overwrite = F, append = T, row.names=F)
dbWriteTable(dbcon, "Journal", Journal_df, overwrite = F, append = T, row.names=F)
dbWriteTable(dbcon, "Author", Author_df, overwrite = F, append = T, row.names=F)
dbWriteTable(dbcon, "Article", Article_df, overwrite = F, append = T, row.names=F)
dbWriteTable(dbcon, "Author_Collab", Author_Collab_df, overwrite = F, append = T, row.names=F)



