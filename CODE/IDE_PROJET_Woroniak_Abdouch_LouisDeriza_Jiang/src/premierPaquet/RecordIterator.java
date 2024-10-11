package premierPaquet;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/*
 * Représente un générateur de record prenant en compte certaines conditions 
 */
public class RecordIterator {
	/*
	 * Les conditions du generateur
	 */
    private SelectCondition condition;
    
    /*
	 * Le rid du dernier record générés
	 */
    private RecordId ridLastRecord;
    
    /*
     * indique si le générrateur est utilisé pour une jointure ou un select
     */ 
    private boolean pourUneJointure;
    
    /*
     * Construit un RecordIterator à partir d'une condition
     * @param condition la condition du RecordIterator
     */ 
    public RecordIterator(SelectCondition condition) {
    	this.condition = condition;
        this.ridLastRecord = null;
        this.pourUneJointure = false;
    }
    
     /*
     * Construit un RecordIterator à partir d'une condition, du rid du record de départ, et un indicateur de jointure
     * @param condition la condition du RecordIterator
     * @param ridLastRecord rid du record à partir duquel on souhaite initialiser le générateur de record
     * @param pourUneJointure indicateur de jointure
     */
    public RecordIterator(SelectCondition condition, RecordId ridLastRecord, boolean pourUneJointure) {
    	this.condition = condition;
    	this.ridLastRecord = ridLastRecord;
    	this.pourUneJointure = pourUneJointure;
    }
    
    /*Cette méthode teste si le record passé en paramètre respecte les conditions(en attributs de ce RecordItertor)
     * @param record le Record sur lequel tester les conditions
     * @param estUneJointure le boolean à true si on teste pour une jointure, à false sinon
     * @return un boolean true si le record respecte les conditions false sinon
     */
    private  boolean recordRespecteLesConditions(Record record, boolean estUneJointure){
    	ArrayList <String> listeOperateurs = this.condition.getOperateur();
    	RelationInfo relation = this.condition.getRelation();
    	if(relation!=null && listeOperateurs == null) {//Si il n'y a pas de condition on renvoie true
    		return(true);
    	}
    	ArrayList <String> valeursDeComparaison = this.condition.getValeurDeComparaison();
    	ArrayList<Integer> indiceColonnes = this.condition.getIndiceColonnes();
    	if(!estUneJointure) {//Si on ne teste pas pour une jointure
    		for(int i = 0; i<listeOperateurs.size(); i++) {
        		if(relation.getListeDesColonnes().get(indiceColonnes.get(i)).getType().equals("INTEGER")){//Si la colonne de la condition contient des entiers
    				//Pour chaque opérateur on teste si la valeur du Record respecte la "relation d'ordre" avec la valeur de la commande
        			//comme ça on tombera forcément sur l'opérateur de la commande à un moment et on renvoie true ou false en fonction
        			//de si oui ou non la valeur dans le record  respecte la relation d'ordre avec la valeur passée dans la commande
    				if(listeOperateurs.get(i).equals("<=")){
    					if(!(Integer.parseInt(record.getValues().get(indiceColonnes.get(i)))<= Integer.parseInt(valeursDeComparaison.get(i)))) {
    						return(false);
    					}
    					
    				}
    				else if(listeOperateurs.get(i).equals(">=")){
    					if(!(Integer.parseInt(record.getValues().get(indiceColonnes.get(i))) >= Integer.parseInt(valeursDeComparaison.get(i)))) {
    						return(false);
    					}
    				}
    				else if(listeOperateurs.get(i).equals("<>")){
    					if(!(Integer.parseInt(record.getValues().get(indiceColonnes.get(i))) != Integer.parseInt(valeursDeComparaison.get(i)))) {
    						return(false);
    					}
    				}
    				else if(listeOperateurs.get(i).equals("<")){
    					if(!(Integer.parseInt(record.getValues().get(indiceColonnes.get(i))) < Integer.parseInt(valeursDeComparaison.get(i)))) {
    						return(false);
    					}
    				}
    				else if(listeOperateurs.get(i).equals(">")){
    					if(!(Integer.parseInt(record.getValues().get(indiceColonnes.get(i))) > Integer.parseInt(valeursDeComparaison.get(i)))) {
    						return(false);
    					}
    				}
    				else {
    					if(!(Integer.parseInt(record.getValues().get(indiceColonnes.get(i))) == Integer.parseInt(valeursDeComparaison.get(i)))) {
    						return(false);
    					}
    				}
        		}
        		
        		//On fait la même chose que pour les entiers sauf qu'on prends en compte dans les tests qu'on test sur des réels
        		else if(relation.getListeDesColonnes().get(indiceColonnes.get(i)).getType().equals("REAL")) {
                    if(listeOperateurs.get(i).equals("<=")){
                    	if(!(Float.parseFloat(record.getValues().get(indiceColonnes.get(i)))<= Float.parseFloat(valeursDeComparaison.get(i)))) {
    						return(false);
    					}
    				}
    				else if(listeOperateurs.get(i).equals(">=")){
    					if(!(Float.parseFloat(record.getValues().get(indiceColonnes.get(i)))>= Float.parseFloat(valeursDeComparaison.get(i)))) {
    						return(false);
    					}
    				}
    				else if(listeOperateurs.get(i).equals("<>")){
    					if(!(Float.parseFloat(record.getValues().get(indiceColonnes.get(i))) != Float.parseFloat(valeursDeComparaison.get(i)))) {
    						return(false);
    					}
    				}
    				else if(listeOperateurs.get(i).equals("<")){
    					if(!(Float.parseFloat(record.getValues().get(indiceColonnes.get(i))) < Float.parseFloat(valeursDeComparaison.get(i)))) {
    						return(false);
    					}
    				}
    				else if(listeOperateurs.get(i).equals(">")){
    					if(!(Float.parseFloat(record.getValues().get(indiceColonnes.get(i))) > Float.parseFloat(valeursDeComparaison.get(i)))) {
    						return(false);
    					}
    				}
    				else {
    					if(!(Float.parseFloat(record.getValues().get(indiceColonnes.get(i))) == Float.parseFloat(valeursDeComparaison.get(i)))) {
    						return(false);
    					}
    				}
        		}
        		
        		//De même pour les chaînes de caractères, les comparaisons se font par ordre alphabétique
        		//pour 2 String a,b si a<b (dans l'ordre alphabétique) a.compareTo(b)<0
        		//Si a>b a.comparaTo(b) >0
        		// si a et b sont égaux, a.compareTo(b) = 0
        		else {
        			if(listeOperateurs.get(i).equals("<=")){
        				if(!((record.getValues().get(indiceColonnes.get(i))).compareTo(valeursDeComparaison.get(i)) <= 0)) {
        					return false;
        				}
        			}
        			
        			else if(listeOperateurs.get(i).equals(">=")){
        				if(!((record.getValues().get(indiceColonnes.get(i))).compareTo(valeursDeComparaison.get(i)) >= 0)) {
        					return false;
        				}
        			}
        			
        			else if(listeOperateurs.get(i).equals("<>")){
        				if(!((record.getValues().get(indiceColonnes.get(i))).compareTo(valeursDeComparaison.get(i)) != 0)) {
        					return false;
        				}
        			}
        			
        			else if(listeOperateurs.get(i).equals("<")){
        				if(!((record.getValues().get(indiceColonnes.get(i))).compareTo(valeursDeComparaison.get(i)) < 0)) {
        					return false;
        				}
        			}
        			
        			else if(listeOperateurs.get(i).equals(">")){
        				if(!((record.getValues().get(indiceColonnes.get(i))).compareTo(valeursDeComparaison.get(i)) > 0)) {
        					return false;
        				}
        			}
        			
        			else {
        				if(!((record.getValues().get(indiceColonnes.get(i))).compareTo(valeursDeComparaison.get(i)) == 0)) {
        					return false;
        				}
        			}
    		    }
        	}
        	//Si toutes les conditions sont respectées, on retourne vrai
        	return(true);
    	}
    	else {//Si c'est pour une jointure
			//On fait la même choses sauf qu'on inverse les valeurs de comparaisons
    		for(int i = 0; i<listeOperateurs.size(); i++) {
        		if(relation.getListeDesColonnes().get(indiceColonnes.get(i)).getType().equals("INTEGER")){
    				if(listeOperateurs.get(i).equals("<=")){
    					if(!(Integer.parseInt(valeursDeComparaison.get(i))<=Integer.parseInt(record.getValues().get(indiceColonnes.get(i))) )) {
    						return(false);
    					}
    				}
    				else if(listeOperateurs.get(i).equals(">=")){
    					if(!(Integer.parseInt(valeursDeComparaison.get(i))>=Integer.parseInt(record.getValues().get(indiceColonnes.get(i))) )) {
    						return(false);
    					}
    				}
    				else if(listeOperateurs.get(i).equals("<>")){
    					if(!(Integer.parseInt(valeursDeComparaison.get(i))!=Integer.parseInt(record.getValues().get(indiceColonnes.get(i))) )) {
    						return(false);
    					}
    				}
    				else if(listeOperateurs.get(i).equals("<")){
    					if(!(Integer.parseInt(valeursDeComparaison.get(i)) < Integer.parseInt(record.getValues().get(indiceColonnes.get(i))) )) {
    						return(false);
    					}
    					
    				}
    				else if(listeOperateurs.get(i).equals(">")){
    					if(!(Integer.parseInt(valeursDeComparaison.get(i))>Integer.parseInt(record.getValues().get(indiceColonnes.get(i))) )) {
    						return(false);
    					}
    				}
    				else {
    					if(!(Integer.parseInt(valeursDeComparaison.get(i)) == Integer.parseInt(record.getValues().get(indiceColonnes.get(i))))) {
    						return(false);
    					}
    				}
    				
    				
        		}
        		else if(relation.getListeDesColonnes().get(indiceColonnes.get(i)).getType().equals("REAL")) {
                    if(listeOperateurs.get(i).equals("<=")){
                    	if(!(Float.parseFloat(valeursDeComparaison.get(i))<=Float.parseFloat(record.getValues().get(indiceColonnes.get(i))))) {
    						return(false);
    					}
    				}
    				else if(listeOperateurs.get(i).equals(">=")){
    					if(!(Float.parseFloat(valeursDeComparaison.get(i))>=Float.parseFloat(record.getValues().get(indiceColonnes.get(i))))) {
    						return(false);
    					}
    				}
    				else if(listeOperateurs.get(i).equals("<>")){
    					if(!(Float.parseFloat(valeursDeComparaison.get(i))!=Float.parseFloat(record.getValues().get(indiceColonnes.get(i))))) {
    						return(false);
    					}
    				}
    				else if(listeOperateurs.get(i).equals("<")){
    					if(!(Float.parseFloat(valeursDeComparaison.get(i))< Float.parseFloat(record.getValues().get(indiceColonnes.get(i))))) {
    						return(false);
    					}
    				}
    				else if(listeOperateurs.get(i).equals(">")){
    					if(!(Float.parseFloat(valeursDeComparaison.get(i)) > Float.parseFloat(record.getValues().get(indiceColonnes.get(i))))) {
    						return(false);
    					}
    				}
    				else {
    					if(!(Float.parseFloat(record.getValues().get(indiceColonnes.get(i))) == Float.parseFloat(valeursDeComparaison.get(i)))) {
    						return(false);
    					}
    				}
        		}
        		else {
        			if(listeOperateurs.get(i).equals("<=")){
        				if(!((valeursDeComparaison.get(i)).compareTo(record.getValues().get(indiceColonnes.get(i))) <= 0)) {
        					return false;
        				}
        			}
        			else if(listeOperateurs.get(i).equals(">=")){
        				if(!((valeursDeComparaison.get(i)).compareTo(record.getValues().get(indiceColonnes.get(i))) >= 0)) {
    						return(false);
    					}
    				}
        			else if(listeOperateurs.get(i).equals("<>")){
        				if(!((valeursDeComparaison.get(i)).compareTo(record.getValues().get(indiceColonnes.get(i))) != 0)) {
    						return(false);
    					}
    				}
        			else if(listeOperateurs.get(i).equals("<")){
        				if(!((valeursDeComparaison.get(i)).compareTo(record.getValues().get(indiceColonnes.get(i))) < 0)) {
    						return(false);
    					}
    				}
        			else if(listeOperateurs.get(i).equals(">")){
        				if(!((valeursDeComparaison.get(i)).compareTo(record.getValues().get(indiceColonnes.get(i))) > 0)) {
    						return(false);
    					}
    				}
        			else {
        				if(!((valeursDeComparaison.get(i)).compareTo(record.getValues().get(indiceColonnes.get(i))) == 0)) {
        					return(false);
        				}
        			}
    		    }
        		
        	}
        	return(true);
    	}
    	
    }
    /*
     *genère un record respectant les conditions d'un jointure ou d'un Select
     * et conserve/met à jour les informations essentielle pour la reutilisation de cette
     * méthode
     * @return Le record correspondant, ou null
     *  
     */
    public Record nextRecord() {    	
    	BufferManager bufferMana = BufferManager.getBufferManager();
    	ByteBuffer bufferHeaderPage = bufferMana.GetPage(condition.getRelation().getHeaderPageId());
    	bufferHeaderPage.position(0);
		int nbDepage = bufferHeaderPage.getInt();
		if(nbDepage <=0) {
			return null;
		}
		ByteBuffer bufferDataPage;
		int iterationDeDepartDansHeaderPage;
		int iterationDeDepartDuSlotDataPage;
		int fileIdDataPage;
		int pageIdDataPage;
    	if(ridLastRecord == null) {
    		fileIdDataPage = bufferHeaderPage.getInt();
    		pageIdDataPage = bufferHeaderPage.getInt();
    		bufferHeaderPage.getInt();
    		bufferDataPage = bufferMana.GetPage(new PageId(fileIdDataPage,pageIdDataPage));
    		iterationDeDepartDansHeaderPage = 0;
    		iterationDeDepartDuSlotDataPage = 0;
    		
    	}

    	else{
    		bufferDataPage = bufferMana.GetPage(ridLastRecord.getPageId());
    		fileIdDataPage = ridLastRecord.getPageId().getFileIdx();
    		pageIdDataPage = ridLastRecord.getPageId().getPageIdx();
    		iterationDeDepartDansHeaderPage = -1;
    		boolean trouve = false;
    		int testIntHeaderPageFileId;
    		int testIntHeaderPagePageId;
    		while(trouve == false) {
    			testIntHeaderPageFileId = bufferHeaderPage.getInt();
    			testIntHeaderPagePageId = bufferHeaderPage.getInt();
    			if (fileIdDataPage == testIntHeaderPageFileId && pageIdDataPage == testIntHeaderPagePageId ) {
    				trouve = true;
        		}
    			iterationDeDepartDansHeaderPage++;
    			bufferHeaderPage.getInt();
    			
    		}
    		iterationDeDepartDuSlotDataPage = ridLastRecord.getSlotIdx()+1;
    	}
    	
    	Record recordArenvoyer;
    	
		int nbRecord;
		int positionEcritureDuSlot;
		
		for(int i = iterationDeDepartDansHeaderPage; i<nbDepage; i++) {
			nbRecord =  bufferDataPage.getInt(DBParams.PageSize-8);
			positionEcritureDuSlot = DBParams.PageSize -16;
			if(i == iterationDeDepartDansHeaderPage) {
				if(ridLastRecord!= null) {
					positionEcritureDuSlot -= (ridLastRecord.getSlotIdx()+1)*8;
				}
				
				for(int j = iterationDeDepartDuSlotDataPage; j<nbRecord; j++) {
					recordArenvoyer = new Record(condition.getRelation(), Record.readFromBufferOffset(bufferDataPage, bufferDataPage.getInt(positionEcritureDuSlot)) , Record.readFromBufferValues(condition.getRelation(), bufferDataPage, bufferDataPage.getInt(positionEcritureDuSlot)));
					if(recordRespecteLesConditions(recordArenvoyer,pourUneJointure) == true) {
						//changer ridLastRecord
						if(ridLastRecord!= null) {
							ridLastRecord = new RecordId(new PageId(fileIdDataPage,pageIdDataPage), ridLastRecord.getSlotIdx() + (j - ridLastRecord.getSlotIdx()));
						}
						else {
							ridLastRecord = new RecordId(new PageId(fileIdDataPage,pageIdDataPage), j);
						}
						//Liberer le bufferManager des 2 pages en cours
						bufferMana.FreePage(new PageId(fileIdDataPage, pageIdDataPage), false);
						bufferMana.FreePage(condition.getRelation().getHeaderPageId(), false);
						
						return(recordArenvoyer);
						
					}
					positionEcritureDuSlot-=8;
				}
			}
			else {
				
				for(int j =0; j<nbRecord; j++) {
					recordArenvoyer = new Record(condition.getRelation(), Record.readFromBufferOffset(bufferDataPage, bufferDataPage.getInt(positionEcritureDuSlot)), Record.readFromBufferValues(condition.getRelation(), bufferDataPage, bufferDataPage.getInt(positionEcritureDuSlot)));
					if(recordRespecteLesConditions(recordArenvoyer,pourUneJointure) == true) {
						ridLastRecord = new RecordId(new PageId(fileIdDataPage,pageIdDataPage), j);
						bufferMana.FreePage(new PageId(fileIdDataPage, pageIdDataPage), false);
						bufferMana.FreePage(condition.getRelation().getHeaderPageId(), false);						
						return(recordArenvoyer);
					}
					positionEcritureDuSlot-=8;
				}
			}
			
			
			//Liberer le bufferManager de l'ancienne data page
			
			bufferMana.FreePage(new PageId(fileIdDataPage, pageIdDataPage), false);
			if(i != nbDepage -1) {
				fileIdDataPage = bufferHeaderPage.getInt();
	    		pageIdDataPage = bufferHeaderPage.getInt();
	    		bufferHeaderPage.getInt();
	    		bufferDataPage = bufferMana.GetPage(new PageId(fileIdDataPage,pageIdDataPage));
			}
	    	
	    }
		bufferMana.FreePage(condition.getRelation().getHeaderPageId(), false);
		return(null);
    	
    }
    
    /*
     * genère tous les records (respectant la/les conditions) d'une page 
     * et conserve/met à jour les informations essentielle pour la reutilisation de cette
     * méthode
     * @param datapage la page sur laquelle on applique la méthode
     * @return le record de la page, ou null
     *  
     */
    public Record nextRecordOfDataPage(PageId datapage) {
    	
    	BufferManager bufferMana = BufferManager.getBufferManager();

		ByteBuffer bufferDataPage;
		int iterationDeDepartDuSlotDataPage;
		bufferDataPage = bufferMana.GetPage(datapage);
		
		if(bufferDataPage.getInt(DBParams.PageSize-8)<=0) {
			bufferMana.FreePage(datapage, false);
		    return null;
	    }
		
    	if(ridLastRecord == null) {
    		iterationDeDepartDuSlotDataPage = 0;
    	}
    	
    	else{
    		iterationDeDepartDuSlotDataPage = ridLastRecord.getSlotIdx() +1;
    	}
    	
    	Record recordArenvoyer;
    	
		int nbRecord;
		int positionEcritureDuSlot;
		


		nbRecord =  bufferDataPage.getInt(DBParams.PageSize-8);
		positionEcritureDuSlot = DBParams.PageSize -16;
		if(ridLastRecord!= null) {
			positionEcritureDuSlot -= (ridLastRecord.getSlotIdx()+1)*8;
		}
				
		for(int j = iterationDeDepartDuSlotDataPage; j<nbRecord; j++) {
			recordArenvoyer = new Record(condition.getRelation(), Record.readFromBufferOffset(bufferDataPage, bufferDataPage.getInt(positionEcritureDuSlot)) , Record.readFromBufferValues(condition.getRelation(), bufferDataPage, bufferDataPage.getInt(positionEcritureDuSlot)));
			
			if(recordRespecteLesConditions(recordArenvoyer,pourUneJointure) == true) {
				//changer ridLastRecord
				if(ridLastRecord!= null) {
					ridLastRecord = new RecordId(datapage, (ridLastRecord.getSlotIdx() + (j - ridLastRecord.getSlotIdx()) ));
				}
				else {
					ridLastRecord = new RecordId(datapage, j);
				}
				//Liberer le bufferManager des 2 pages en cours
				bufferMana.FreePage(datapage, false);
				
				return(recordArenvoyer);
						
			}
			positionEcritureDuSlot-=8;
		}
			
	    //Liberer le bufferManager de l'ancienne data page
			
	    
        bufferMana.FreePage(datapage, false);
		return(null);
    	
    }
    
    /*
     * permet d'obtenir les conditions du recordIterator
     * @return la/les condition(s) de l'itérateur
     */
    public SelectCondition getCondition() {
    	return condition;
    }

    /*
     * permet d'obtenir le rid du dernier record généré
     * @return le rid du dernier record généré
     */ 
	public RecordId getRidLastRecord() {
		return ridLastRecord;
	}

    /*
     * permet de remplacer le rid du dernier record généré
     * @param ridLastRecord le nouveau rid
     */
	public void setRidLastRecord(RecordId ridLastRecord) {
		this.ridLastRecord = ridLastRecord;
	}
	
	/*Cette méthode permet de connaitre le RecordId du prochain Record qui resepecte les conditions de l'itérateur dans une page donnée, afin 
	 * de le supprimer avec le delete
	 * @param pageId le PageId de la page à parcourir
	 * @return RecordId le RecordId du prochain Record à respecter les conditions
	 */
	public RecordId nextRecordPage(PageId pageId) {
    	BufferManager bufferMana = BufferManager.getBufferManager();
    	ByteBuffer bufferDataPage = bufferMana.GetPage(pageId);
    	
    	int positionEcritureDuSlot;
		positionEcritureDuSlot = DBParams.PageSize -16; //Position du début du premier record dans la page
		if(ridLastRecord!= null) { //Si on à déjà trouvé un record qui respectait les conditions 
			positionEcritureDuSlot -= ridLastRecord.getSlotIdx()*8; //On place la position de début au niveau de l'ancien record qui a été supprimé
		}else{ //Sinon on note l'ancien Record respectant les conditions comme étant le premier de la page
			   //ce n'est pas forcément le cas mais cela sert à la suite de l'éxécution 
    		ridLastRecord = new RecordId(pageId,0);
    	}
    	
    	int iterationDeDepartDuSlotDataPage = ridLastRecord.getSlotIdx();
    	Record recordATester;
		int nbRecord;
		
		nbRecord =  bufferDataPage.getInt(DBParams.PageSize-8);
		for(int i = iterationDeDepartDuSlotDataPage; i < nbRecord; i++) {
			recordATester = new Record(condition.getRelation(), Record.readFromBufferOffset(bufferDataPage, bufferDataPage.getInt(positionEcritureDuSlot)) , Record.readFromBufferValues(condition.getRelation(), bufferDataPage, bufferDataPage.getInt(positionEcritureDuSlot)));
			if(recordRespecteLesConditions(recordATester,false) == true) {
				ridLastRecord = new RecordId(pageId, ridLastRecord.getSlotIdx() + (i - ridLastRecord.getSlotIdx()));
				bufferMana.FreePage(pageId, false);
				return(ridLastRecord);
				
			}
			positionEcritureDuSlot-=8;
		}
	    bufferMana.FreePage(pageId, false);
	    return null;
    }
    
    
	/*
	 * Cette méthode permet de remettre à null la valeur du RecordId : ridLastRecord
	 */
    public void resetRidLastRecord() {
    	ridLastRecord = null;
    }

}
	
    
    

