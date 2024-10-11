 package premierPaquet;

import java.io.Serializable;
import java.lang.String;
/**
 * Represente les colonnes d'une relation
 * 
 * @author Mounir Abdouch, Grégoire woroniak, Socrate Louis Deriza, Olivier Jiang
 *
 */
public class ColInfo implements Serializable{
	
	/**
	 * numero associe a notre classe serialisable
	 */
	private static final long serialVersionUID = 3402126011377726752L;
	
	/**
	 * le nom de la colonne
	 */
	private String nomColonne;
	
	/**
	 * le type de la colonne
	 */
	private String type;
	
	/**
	 * 
	 * Construit une colonne dont le nom et le type sont donnés
	 * 
	 * @param nomColonne nom de la colonne
	 * @param nomType type de la colonne
	 */
	public ColInfo(String nomColonne, String nomType){
		if(nomType.contains("VARCHAR(") == true || nomType.equals("INTEGER") || nomType.equals("REAL")) { 
			this.nomColonne = nomColonne;
			this.type = nomType;
		}
		else {
			System.out.println("Votre type n'est disponible");
		}
	}

	/**
	 * 
	 * Permet d'obtenir le nom de la colonne
	 * 
	 * @return le nom de la colonne
	 */
	public String getNomColonne() {
		return nomColonne;
	}
	
	/**
	 * Permet de modifier le nom de la colonne
	 * 
	 * @param nomColonne le nom de la colonne
	 */
	public void setNomColonne(String nomColonne) {
		this.nomColonne = nomColonne;
	}

	/**
	 * 
	 * Permet d'obtenir le type de la colonne
	 * 
	 * @return le type de la colonne
	 */
	public String getType() {
		return type;
	}

	/**
	 * Permet de modifier le type de la colonne
	 * 
	 * @param type le type de la colonne
	 */
	public void setType(String type) {
		this.type = type;
	}
	
	/**
	 * Redéfinition de la méthode toString() de la classe Object
	 * Crée une chaîne de caractère représentant l'ensemble des informations d'un objet ColInfo
	 * @return la chaîne de caratère récapitulant les informations
	 */
	@Override
	public String toString() {
		StringBuilder strb = new StringBuilder("nom colonne: ");
		strb.append(nomColonne+"\n");
		strb.append("type colonne: ");
		strb.append(type);
		return(strb.toString());
	}

}

