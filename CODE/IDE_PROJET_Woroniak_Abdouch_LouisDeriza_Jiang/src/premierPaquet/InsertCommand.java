package premierPaquet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * Classe qui gere la commande INSERT
 * 
 * @author Mounir Abdouch, Grégoire woroniak, Socrate Louis Deriza, Olivier Jiang
 * 
 */
public class InsertCommand {
	
	/**
	 * Boolean qui permet de savoir si on souhaite realiser un insert a travers un fichier ou non
	 */
	private boolean insertFromFile;
	
	/**
	 * Liste contenant les records crees via un fichier
	 */
	private ArrayList<Record> listeRecord;
	
	/**
	 * La relation dans laquelle on souhaite faire le INSERT
	 */
	private String nomRelation;
	
	/**
	 * Liste contenant les valeurs d'un record
	 */
	private ArrayList<String> valeurs;
	
	/**
	 * Construit un ou plusieurs record 
	 * 
	 * @param commande la commande ecrite par l'utilisateur
	 */
	public InsertCommand(String commande) {
		StringTokenizer st  = new StringTokenizer(commande,"() ,;");
		st.nextToken();
		st.nextToken();
	    this.nomRelation = st.nextToken();
	    String typeInsert;
	    typeInsert =st.nextToken();
		if(typeInsert.equals("VALUES")) {
			insertFromFile = false;
			valeurs = new ArrayList<>();
		    while(st.hasMoreElements()) {
		    	valeurs.add(st.nextToken());
		    }
		}
		else if(typeInsert.equals("FILECONTENTS")) {
			this.insertFromFile = true;
			String nomFichier = st.nextToken();
			/*String cheminPrincipalProjet = (String) DBParams.DBPath.subSequence(0,DBParams.DBPath.length()-3);
			File file = new File(cheminPrincipalProjet+"/"+nomFichier);*/
			File file = new File(nomFichier);
			FileReader fr;

			BufferedReader br;
			try {
				fr = new FileReader(file);
				br = new BufferedReader(fr);  

				listeRecord = new ArrayList<Record>();
				String line;
				Record r ;
				while((line = br.readLine()) != null) {
					StringTokenizer uneRelation = new StringTokenizer(line,", ");
					valeurs = new ArrayList<>();
					while(uneRelation.hasMoreTokens()) {
					    valeurs.add(uneRelation.nextToken());
					}
					RelationInfo rel = Catalog.getCatalog().GetRelationInfo(nomRelation);
					r = new Record(rel,valeurs);
					listeRecord.add(r);
				}
				br.close();
				fr.close();
			} catch (FileNotFoundException e) {
				System.out.println("Le fichier "+nomFichier+" est introuvable");
				listeRecord = null;
				valeurs = null;
				return ;
			} catch (IOException e) {
				System.out.println("Une erreur d'entrée/sortie fichier est apparu lors du traitement du fichier "+nomFichier+".");
				listeRecord = null;
				valeurs = null;
				return ;
			}        
		}
		else {
			System.out.println("Commande INSERT mal ecrite !");
		}
	}
		
	/**
	 * Permet d'inserer les records crees dans le SGBD
	 * 
	 */
	public void execute() {
		if(insertFromFile == false) {
			System.out.println(valeurs);
			Catalog catalog = Catalog.getCatalog();
			FileManager filemana = FileManager.getFileManager();
			RelationInfo relInfo = catalog.GetRelationInfo(nomRelation);
			for (ColInfo col: relInfo.getListeDesColonnes()) {
				System.out.println(col);
			}
			Record nouvelleInstance = new Record(relInfo,valeurs);
			filemana.InsertRecordIntoRelation(nouvelleInstance);
		}
		if(insertFromFile == true) {
			if(listeRecord!=null) {
				for(int i=0;i<this.listeRecord.size();i++) {
					FileManager.getFileManager().InsertRecordIntoRelation(listeRecord.get(i));
				}
				System.out.println("INSERT via fichier termine");
			}
			
		}
		BufferManager bufferMana = BufferManager.getBufferManager();
		bufferMana.Finish();
		DiskManager diskMana = DiskManager.getDiskManager();
		diskMana.Finish();
		
	}
}
