package premierPaquet;
import java.util.ArrayList;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.File;
import java.io.FileNotFoundException;

/**
 * Classe Catalog qui contient toutes les informations concernant l'ensemble des relations de notre base de donnees
 * 
 * @author Mounir Abdouch, Grégoire woroniak, Socrate Louis Deriza, Olivier Jiang
 *
 */
public class Catalog implements Serializable{
	
	/**
	 * numero associe a notre classe serialisable
	 */
	private static final long serialVersionUID = 1715774098247501883L;
	
	/**
	 * liste contenant la liste des relations
	 */
	private ArrayList <RelationInfo> ListeDesRelations;
	
	/**
	 * l'unique instance de notre catalog
	 */
	private static Catalog CatalogueInstance = new Catalog();
	
	/**
	 * Construit un unique Catalog 
	 */
	private Catalog() {
		ListeDesRelations = new ArrayList<RelationInfo>();
	}
	
	/**
	 * Permet d'avoir qu'une seule instance du Catalog
	 * 
	 * @return  l'unique instance du Catalog
	 */
	public static Catalog getCatalog() {
		return (CatalogueInstance);
	}
	
	/**
	 * Permet d'initialiser le Catalog, lit le fichier Catalog.sv et remplit ensuite le Catalog
	 * 
	 */
	public void Init() {
		String nomFichier = "Catalog.sv";
		File fichierCatalogsv = new File(DBParams.DBPath+"/"+nomFichier);
		if(fichierCatalogsv.exists() == true) {
			FileInputStream fisCatalogsv = null;
			ObjectInputStream oisCatalogsv = null;
			try {
				fisCatalogsv = new FileInputStream(fichierCatalogsv);
				try {
					oisCatalogsv = new ObjectInputStream(fisCatalogsv);
				} catch (IOException e) {
					System.out.println("Erreur d'entrée/sortie initilialisation ObjectInputStream: "+e.getMessage());
					e.printStackTrace();
				}
				try {
					Catalog.setCatalogueInstance((Catalog) oisCatalogsv.readObject());
					System.out.println("\nRécuperation du catalogue (base de données) à partir du fichier de sauvegarde \"Catalog.sv\"");
				} catch (ClassNotFoundException | IOException e) {
					
					e.printStackTrace();
				}
				
			}catch(FileNotFoundException e) {
				System.out.println("Fichier non trouvé: "+e.getMessage());
				e.printStackTrace();
			}
		}
		else {
			System.out.println("\nInitialisation de la base de données\n");
			fichierCatalogsv.delete();
		}
	}
	
	/**
	 * Sauvegarde les informations du Catalog dans un fichier nomme Catalog.sv
	 * 
	 */
	public void Finish() {
		String nomFichier = "Catalog.sv";
		FileOutputStream fosCatalogsv = null;
		ObjectOutputStream oosCatalogsv = null;
		File fichierCatalogsv = new File(DBParams.DBPath+"/"+nomFichier);
		try {
			fosCatalogsv = new FileOutputStream(fichierCatalogsv);

			try {
				oosCatalogsv = new ObjectOutputStream(fosCatalogsv);
			} catch (IOException e) {
				System.out.println("Erreur d'entrée/sortie initilialisation ObjectOutputStream: "+e.getMessage());
				e.printStackTrace();
			}
			
			try {
				oosCatalogsv.writeObject(Catalog.getCatalog());
				
			} catch (IOException e) {
				System.out.println("Erreur d'entrée/sortie écriture ObjectOutputStream: "+e.getMessage());
				e.printStackTrace();
			}
			
		}catch(FileNotFoundException e) {
			System.out.println("Fichier non trouvé: "+e.getMessage());
		}
		
        
	}
	/** 
	 * Permet d'ajouter une relation dans la liste des relations
	 * 
	 * @param r une relation
	 * 
	 */
	public void AddRelationInfo(RelationInfo r) {
		this.ListeDesRelations.add(r);
	}
	
	/**
	 * Permet de verifier que la relation existe dans la liste des relations, dans la base de donnees
	 * 
	 * @param nomDeLaRelation la relation que l'on cherche dans la liste des relations
	 * @return la relation si elle est existe dans la liste des relations sinon null
	 */
	public RelationInfo GetRelationInfo(String nomDeLaRelation) {
		int i =0;
		boolean trouve = false;
		while(trouve== false && i<ListeDesRelations.size()) {
			if(ListeDesRelations.get(i).getNomRelation().equals(nomDeLaRelation) == true) {
				trouve = true;
			}
			else {
				i++;
			}
		}
		if (trouve == true) {
			return(ListeDesRelations.get(i));
			
		}
		else {
			System.out.println("\nLa relation "+nomDeLaRelation+" ne se trouve pas dans la base de données ");
			return(null);
		}
	}
	
	/**
	 * Permet de supprimer le fichier catalog.sv si il existe et ainsi reinitialiser le catalog
	 * 
	 */
	public void reset() {
		this.ListeDesRelations.clear();
		File fichierASupprimer;
		String cheminFichier = DBParams.DBPath+"/"+"Catalog.sv";
        fichierASupprimer = new File(cheminFichier);
        if(fichierASupprimer.delete() == false) {
        	System.out.println("Catalog.sv n'existant pas, il ne sera supprimé... -> méthode reset() du BufferManager");
        }
	}
	
	/**
	 * 
	 * Permet de remplacer l'instance du Catalog par celle en parametre
	 * 
	 * @param catalogueInstance l'instance du Catalog
	 */
	private static void setCatalogueInstance(Catalog catalogueInstance) {
		CatalogueInstance = catalogueInstance;
	}

}
