package premierPaquet;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Représente le generateur de records respectant les conditions de la jointure
 * 
 * @author Mounir Abdouch, Grégoire woroniak, Socrate Louis Deriza, Olivier Jiang
 *
 */
public class RecordIteratorJoin {
	
	/**
	 * Structure de données prenant en compte tous les paramètres cruciaux d'une commande de jointure 
	 */
	private SelectConditionJoin condition;
	
	/*
	 * Rid du dernier record [de la premiere relation (relation externe)] affiché par la méthode execute() de JoinCommand
	 */
    private RecordId ridLastRecordFirstRelation;
    
    /*
	 * Rid du dernier record [de la premiere relation (relation externe)] affiché par la méthode execute() de JoinCommand
	 * AVEC UN DÉCALLAGE DE UN
	 * Miroir = décallage de un
	 */
    private RecordId ridLastRecordFirstRelationMiroir;
    
    /*
	 * Dernier record [de la premiere relation (relation externe)] affiché par la méthode execute() de JoinCommand
	 */
    private Record dernierRecordDeLaRelationExterne;
    
    /*
	 * Dernier record [de la premiere relation (relation externe)] affiché par la méthode execute() de JoinCommand
	 * AVEC UN DÉCALLAGE DE UN
	 * Miroir = décallage de un
	 */ 
    private Record dernierRecordDeLaRelationExterneMiroir;
    
    /*
	 * Rid du dernier record [de la seconde relation (relation interne)] affiché par la méthode execute() de JoinCommand
	 * 
	 */
    private RecordId ridLastRecordSecondRelation;

    /*
     * itération servant à parcourrir les pages du header page (de la relation externe) une et une seule fois
     */
    private int iterationDeDepartDansHeaderPage;
    
    /*
     * Construit un générateur de record à partir de la structure de données prenant en compte tous les paramètres cruciaux d'une commande de jointure
     * 
     * @param condition information de la commande de jointure
     */
    public RecordIteratorJoin(SelectConditionJoin condition) {
    	this.condition = condition;
        this.ridLastRecordFirstRelation = null;
        this.ridLastRecordSecondRelation = null;
        this.iterationDeDepartDansHeaderPage = 0;
        this.dernierRecordDeLaRelationExterne = null;
        this.ridLastRecordFirstRelationMiroir = null;
        this.dernierRecordDeLaRelationExterneMiroir = null;
    }
    
    /*
     * Implémentation de l'algorithme page oriented nested loop join présenté en cours
     * Principe de l'algorithme: Parcours de tous les records d'une data-page de la 1ère relation (relation externe)
     * Pour chaque record de la relation externe, parcourrir TOUS LES RECORDS de la deuxième relation (relation interne) et s'arreter des que
     * l'on trouve un record de la relation interne respectant la/les condition.
     * Si on trouve, mettre à jour les attributs de la classe et retourner le 2 records respectant les conditions
     * Si on ne trouve pas, passer au record suivant de la page en cours (de la relation externe)
     * Si on a finie de parcourrir tous les records de la page en cours, passer à la page suivante à partir du header page (de la relation externe) et répéter l'opération
     * 
     * @return un tableau de 2 records de la relation externe et interne respectant  les conditions de la jointure
     * 
     */
    public Record[] pageOrientedNestedLoopsJoin() {
    	BufferManager bufferMana = BufferManager.getBufferManager();
    	ByteBuffer bufferHeaderPage;
    	bufferHeaderPage = bufferMana.GetPage(condition.getRelation()[0].getHeaderPageId());
    	bufferHeaderPage.position(0);
    	//Recherche du nombre de page de la relation externe
		int nbDepage = bufferHeaderPage.getInt(0);
		if(nbDepage <=0) {
			bufferMana.FreePage(condition.getRelation()[0].getHeaderPageId(), false);
			return null;
		}
		
		bufferMana.FreePage(condition.getRelation()[0].getHeaderPageId(), false);
		//initialisation des variables
	    int variableIterationDeDepartDansHeaderPage = iterationDeDepartDansHeaderPage;
		int fileIdDataPage;
		int pageIdDataPage;
		SelectCondition conditionPourParcourrirTousLesRecords = new SelectCondition(condition.getRelation()[0]);
		RecordIterator recordIteratorPourParcourrirPageParPage  = new RecordIterator(conditionPourParcourrirTousLesRecords,ridLastRecordFirstRelation,false);
		RecordIterator recordIteratorPourParcourrirPageParPageMiroir  = new RecordIterator(conditionPourParcourrirTousLesRecords,ridLastRecordFirstRelationMiroir,false);
		Record recordDeLaRelationExterne;
		Record recordDeLaRelationExterneMiroir;
		Record recordDeLaRelationInterne;
		Record recordDeLaRelationInterneMiroir;
		Record[] lesDeuxRecordsAretourner = new Record[2];
		
		for(int i = variableIterationDeDepartDansHeaderPage;i<nbDepage; i++) {
			//PARCOURS DES RECORDS À PARTIR D'UNE PAGE
			//Recherche de la data page en cours à partir de iterationDeDepartDansHeaderPage 
			bufferHeaderPage = bufferMana.GetPage(condition.getRelation()[0].getHeaderPageId());
			fileIdDataPage = bufferHeaderPage.getInt(4+(iterationDeDepartDansHeaderPage*12));
			pageIdDataPage = bufferHeaderPage.getInt(8+(iterationDeDepartDansHeaderPage*12));
			bufferMana.FreePage(condition.getRelation()[0].getHeaderPageId(), false);
			if(ridLastRecordFirstRelation != null){
				//Si la page actuelle trouvé grâce à iterationDeDepartDansHeaderPage est différente du rid du dernier record affiché par la command execute de JoinCommand
				//Nouvelle page on peut donc re-initialiser les recorIterator les attributs dernierRecordDeLaRelationExterne et dernierRecordDeLaRelationExterneMiroir
				if(ridLastRecordFirstRelation.getPageId().getFileIdx() != fileIdDataPage || ridLastRecordFirstRelation.getPageId().getPageIdx() != pageIdDataPage) {
					recordIteratorPourParcourrirPageParPage.setRidLastRecord(null);
					recordIteratorPourParcourrirPageParPageMiroir.setRidLastRecord(null);
					this.dernierRecordDeLaRelationExterne = null;
					this.dernierRecordDeLaRelationExterneMiroir = null;
				}
			}
			
			recordDeLaRelationExterne = null;
			recordDeLaRelationExterneMiroir = null;
			int t =0;
			
			//Ce premier do while s'arrête lorsque tous les records de la data page en cours de la relation externe auront été parcouru
			do {
				
				if(dernierRecordDeLaRelationExterne!=null && t == 0) {
					recordDeLaRelationExterne = dernierRecordDeLaRelationExterne;
					recordDeLaRelationExterneMiroir = dernierRecordDeLaRelationExterneMiroir;
				}
				else {
					if(dernierRecordDeLaRelationExterne==null && t==0) {
						recordIteratorPourParcourrirPageParPageMiroir.nextRecordOfDataPage(new PageId(fileIdDataPage,pageIdDataPage));
					}
					
					recordDeLaRelationExterne = recordIteratorPourParcourrirPageParPage.nextRecordOfDataPage(new PageId(fileIdDataPage,pageIdDataPage));
					recordDeLaRelationExterneMiroir = recordIteratorPourParcourrirPageParPageMiroir.nextRecordOfDataPage(new PageId(fileIdDataPage,pageIdDataPage));
					this.ridLastRecordSecondRelation = null;

					
					
				}
				if(recordDeLaRelationExterne!=null) {
					ArrayList<String> valeurDeComparaison;
					if(condition.getIndiceColonnesRelation1()!=null) {
						valeurDeComparaison = new ArrayList<>();
						for(int j = 0;j<condition.getIndiceColonnesRelation1().size(); j++) {
	                    	valeurDeComparaison.add(recordDeLaRelationExterne.getValues().get(condition.getIndiceColonnesRelation1().get(j)));
	                    }
					}
					else {
						valeurDeComparaison = null;
					}
					
                    SelectCondition conditionPourParcourrirLesRecordsRespectantLesConditions = new SelectCondition(condition.getRelation()[1], condition.getIndiceColonnesRelation2(), condition.getOperateur(), valeurDeComparaison);
                    RecordIterator recordIterator = new RecordIterator(conditionPourParcourrirLesRecordsRespectantLesConditions, ridLastRecordSecondRelation,true);
                    RecordIterator recordIteratorMiroir = new RecordIterator(conditionPourParcourrirLesRecordsRespectantLesConditions, ridLastRecordSecondRelation,true);
                    recordDeLaRelationInterne = null;
                    recordDeLaRelationInterneMiroir = null;
                    recordIteratorMiroir.nextRecord();
                    
                    //Ce do while dans le do while s'arrête lorsque tous les records de la seconde relation auront été parcourru
                    do {
                    	recordDeLaRelationInterne = recordIterator.nextRecord();
                    	recordDeLaRelationInterneMiroir = recordIteratorMiroir.nextRecord();
                    	
                    	if(nbDepage>1) {
                    		if(recordDeLaRelationExterneMiroir == null && recordDeLaRelationInterne == null && recordDeLaRelationInterneMiroir == null) {
                    			this.iterationDeDepartDansHeaderPage++;
                    			recordDeLaRelationExterne = null;
                    			recordDeLaRelationInterne = null;
                    		}
                    	}
                    	
                    	if(recordDeLaRelationInterne!=null) {
                    			this.ridLastRecordFirstRelation = recordIteratorPourParcourrirPageParPage.getRidLastRecord();
                        		this.ridLastRecordSecondRelation = recordIterator.getRidLastRecord();
                        		this.iterationDeDepartDansHeaderPage = i;
                        		this.ridLastRecordFirstRelationMiroir = recordIteratorPourParcourrirPageParPageMiroir.getRidLastRecord();

                        		this.dernierRecordDeLaRelationExterne = recordDeLaRelationExterne;
                        		this.dernierRecordDeLaRelationExterneMiroir = recordDeLaRelationExterneMiroir;
                        		lesDeuxRecordsAretourner[0] = recordDeLaRelationExterne;
                        		lesDeuxRecordsAretourner[1] = recordDeLaRelationInterne;
                        		return(lesDeuxRecordsAretourner);
                    		
                    	}
                    }while(recordDeLaRelationInterne!=null);
                    
				}
				t++;
			}while(recordDeLaRelationExterne!=null);
		}
		return null;
    }

    /** 
	 * Permet d'obtenir les informations cruciale de la jointure
	 * 
	 * @return la structure de données associé
	 */
	public SelectConditionJoin getCondition() {
		return condition;
	}

	/** 
	 * Permet d'obtenir le rid du dernier record [de la premiere relation (relation externe)] affiché par la méthode execute() de JoinCommand
	 * 
	 * @return le rid
	 */
	public RecordId getRidLastRecordFirstRelation() {
		return ridLastRecordFirstRelation;
	}

	/*
	 *  Permet d'obtenir le rid du dernier record [de la seconde relation (relation interne)] affiché par la méthode execute() de JoinCommand
	 *  
	 *  @return le rid
	 */
	
	public RecordId getRidLastRecordSecondRelation() {
		return ridLastRecordSecondRelation;
	}

	 
	
	/*
	 *  Permet d'obtenir l'itération servant à parcourrir les pages du header page
	 *  
	 *  @return l'entier corresspondant à l'itération
	 */
	public int getIterationDeDepartDansHeaderPage() {
		return iterationDeDepartDansHeaderPage;
	}
    
    

}
