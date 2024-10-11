package premierPaquet;




import java.nio.ByteBuffer;
import java.util.ArrayList;

public class DeleteCommand {
	private RecordIterator recordIterator;
	
	public DeleteCommand(String commande) {
		SelectCondition selectCondition = new SelectCondition(commande);
		this.recordIterator  = new RecordIterator(selectCondition);
	}
	
	/*
	 * Cette méthode supprime tous les records d'unue relation contenu dans l'iterateur en attribut : recordIterator
	 * 
	 * Elle ne prend rien en entrée
	 * 
	 * Elle ne renvoie rien
	 */
	public void deleteAllPages() {
		int totalRecords = 0;
		BufferManager bufferMana = BufferManager.getBufferManager();
		RelationInfo relation = recordIterator.getCondition().getRelation();
		ByteBuffer bufferHeaderPage = bufferMana.GetPage(relation.getHeaderPageId()); // On récupère la headerPage de la relation
		int nbPage = bufferHeaderPage.getInt(0); // nombre de page totale de la relation
		for(int i = 0; i < nbPage; i++) { // pour chacune des pages on delete les records qui respectent les conditions
			int fileIdx = bufferHeaderPage.getInt(4 + i*12);
			int pageIdx = bufferHeaderPage.getInt(8 + i*12);
			PageId pageId = new PageId(fileIdx,pageIdx);
			totalRecords += deletePage(pageId);
			recordIterator.resetRidLastRecord();
		}
		System.out.println("Total deleted records="+totalRecords);
		bufferMana.FreePage(relation.getHeaderPageId(), false); // on libere la headerPage qu'on a pas modifié
	    
	}
	
	
	
	/*
	 * Cette méthode supprime tous les records d'une page de données respectant les conditions contenu dans l'iterateur 
	 * en attribut : recordIterator
	 * 
	 * Elle prend en entrée le PageId de la page a supprimer
	 *  
	 * Elle ne renvoie rien
	 */
	public int deletePage(PageId pageId) {
		RecordId recordId;
		boolean finAffichage =false;
		int totalRecords = 0;
		while(finAffichage == false) { // tant qu'il reste encore des records qui respectent les conditions
			recordId = recordIterator.nextRecordPage(pageId); // prochain record qui respecte les conditions
			if(recordId == null) { // plus de record respectant les conditions on arrête la boucle et on affiche le nb de records supprimés
				finAffichage =true;
			}
			else {
				deleteRecord(pageId, recordId); // suppression du record
				totalRecords++; 
			}
		}
		return totalRecords;
		
	}
	
	/*
	 * Cette méthode supprime un record d'une page de donnée et met à jour la page de donée en question (SlotDirectory,
	 * nombre de record, position d'écriture du prochain record)
	 * 
	 * Elle prend en entrée le RecordId du record à suprimer et la PageId de la page sur laquelle il se trouve
	 * 
	 * Elle ne renvoie rien
	 */
	public void deleteRecord(PageId pageId, RecordId recordId) {
		
		RelationInfo relation = recordIterator.getCondition().getRelation(); // la relation du recordId en parametre
		
		BufferManager bufferMana = BufferManager.getBufferManager();
		FileManager fileMana = FileManager.getFileManager();
		
		ByteBuffer bufferPageId = bufferMana.GetPage(pageId);
		int nombreRecord = bufferPageId.getInt(DBParams.PageSize - 8); // nombre de record de la page avant suppression
		int tailleRecord = bufferPageId.getInt(DBParams.PageSize - 12 - (recordId.getSlotIdx() * 8)); // taille du record à supprimer
		
		for(int i = recordId.getSlotIdx() + 1; i < nombreRecord; i++) { // pour tous les records écrits après celui qu'on supprime
			int posActuelle = bufferPageId.getInt(DBParams.PageSize - 16 - (i*8)); // position du record ième record 
			int tailleActuelle = bufferPageId.getInt(DBParams.PageSize - 12 - (i*8)); // taille du ième record
			
			// On enleve a chaque valeur du offset du ième record la taille du record qu'on supprime
			ArrayList<Integer> changementOffSet = Record.readFromBufferOffset(bufferPageId, posActuelle);
			for(int j = 0; j < changementOffSet.size(); j++) {
				changementOffSet.set(j, changementOffSet.get(j) - tailleRecord);
			}
			
			// On récupère sous forme d'ojet Record le ième record pour le réécrire [tailleRecord] avant
			Record recordADeplacer = new Record(relation, changementOffSet , Record.readFromBufferValues(relation, bufferPageId, posActuelle));
			bufferPageId.putInt(DBParams.PageSize - 4, posActuelle - tailleRecord); 
			fileMana.writeRecordToDataPage(recordADeplacer, pageId,true);
			
			 //mise à jour de la position du ième record dans le slotDirectory
			bufferPageId.putInt(DBParams.PageSize - 16 - ((i-1) * 8), posActuelle - tailleRecord);
			 //mise à jour de la taille du ième record dans le slotDirectory
			bufferPageId.putInt(DBParams.PageSize - 12 - ((i-1) * 8), tailleActuelle);
		}
		
		if(recordId.getSlotIdx() +1 == nombreRecord) { //Si le record à suprimer est le dernier de la page
			// On mets à jour la position d'écriture du prochain record en enlevant seulement sa taille à lui
			bufferPageId.putInt(DBParams.PageSize - 4, bufferPageId.getInt(DBParams.PageSize - 4) - tailleRecord);
		}
		bufferPageId.putInt(DBParams.PageSize - 8,nombreRecord - 1); // On décrémente le nombre de record de la page de un
		bufferMana.FreePage(pageId, true); // On libère la page en précisant qu'on l'a modifié
		ByteBuffer bufferHeaderPage = bufferMana.GetPage(relation.getHeaderPageId());
		boolean trouve = false;
		int i = 4;
		while(trouve == false) { //On cherche la page sur laquelle on à écrit dans la header page
			if(bufferHeaderPage.getInt(i) == pageId.getFileIdx() && bufferHeaderPage.getInt(i+4) == pageId.getPageIdx()) { //Quand on trouve cette page
				int nbOctesLibresDeLaPage = bufferHeaderPage.getInt(i+8);
				bufferHeaderPage.putInt(i+8, nbOctesLibresDeLaPage + tailleRecord + 8); //On incrémente la lace disponible de la page par la taille du record + 8(pour le slot directory)
				trouve = true;
			}
			else {
				i+=12;
			}
		}
		bufferMana.FreePage(relation.getHeaderPageId(), true);
	}

}
