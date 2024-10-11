package premierPaquet;

import java.io.Serializable;
import java.lang.String;
import java.util.ArrayList;
import java.lang.StringBuilder;

/**
 * Represente les différentes caractéristiques relation
 * 
 * @author Mounir Abdouch, Grégoire woroniak, Socrate Louis Deriza, Olivier Jiang
 *
 */
public class RelationInfo implements Serializable{
	
	/**
	 * numero associe a notre classe serialisable
	 */
	private static final long serialVersionUID = 689487832301522774L;
	
	/**
	 * le nom de la relation
	 */
	private String nomRelation;
	
	/**
	 * la liste des colonnes de la relation
	 */
	private ArrayList <ColInfo> ListeDesColonnes;
	
	/**
	 * la page recesant les data pages et leurs espaces restants 
	 */
	private PageId headerPageId;
	
	/*
	 * Construit une relation dont le nom, la liste des colonnes et la headerPageId sont donnés
	 * @param nr le nom de la relation
	 * @param ldc la liste des colonnes de la relation
	 * @param pageIdDeHeaderPage la headerPage
	 */
	public  RelationInfo(String nr,ArrayList<ColInfo> ldc, PageId pageIdDeHeaderPage) {
		this.nomRelation = nr;
		this.ListeDesColonnes = ldc;
		this.headerPageId = pageIdDeHeaderPage;
		
	}
	
	/** 
	 * Permet d'obtenir le nom de la relation
	 * 
	 * @return le nom de la relation
	 */
	public String getNomRelation() {
		return nomRelation;
	}
	
	/**
	 * Permet de modifier le nom de la relation
	 * 
	 * @param nomRelation le nom de la relation
	 */
	public void setNomRelation(String nomRelation) {
		this.nomRelation = nomRelation;
	}
	
	/** 
	 * Permet d'obtenir la liste des colonnes de la relation
	 * 
	 * @return la liste des colonnes de la relation
	 */
	public ArrayList<ColInfo> getListeDesColonnes() {
		return ListeDesColonnes;
	}

	/** 
	 * Permet d'obtenir le nombre de colonne de la relation
	 * 
	 * @return le nombre de colonne de la relation
	 */
	public int nombreDeColonne(){
		return(ListeDesColonnes.size());
	}
	
	/** 
	 * Permet de savoir si une colonne existe ou non
	 * @param nomColonne le nom de la colonne que l'on veut tester
	 * @return le numéro de la colonne si elle existe, null sinon
	 */
	public Integer getNumeroColonne(String nomColonne) {
		int i = 0;
		boolean trouve = false;
		while(i<ListeDesColonnes.size() && trouve == false) {
			if(ListeDesColonnes.get(i).getNomColonne().equals(nomColonne)) {
				trouve = true;
			}
			else {
				i++;
			}
		}
		if(trouve == true) {
			return(i);
		}
		else {
			System.out.println("La colonne "+nomColonne+" n'existe pas dans la relation "+nomRelation);
			return(null);
		}
	}
	
	/** 
	 * Permet d'obtenir la headerPage de la relatin
	 * 
	 * @return la page recesant les data pages et leurs espaces restants
	 */
	public PageId getHeaderPageId() {
		return headerPageId;
	}

	/**
	 * Redéfinition de la méthode toString() de la classe Object
	 * Crée une chaîne de caractère représentant l'ensemble des informations d'un objet RelationInfo
	 * @return la chaîne de caratère récapitulant les informations
	 */
	@Override
	public String toString() {
		StringBuilder strb = new StringBuilder("Nom de la relation: ");
		strb.append(nomRelation);
		strb.append("\n");
		for(int i = 0; i<ListeDesColonnes.size(); i++) {
			strb.append("Colonne n°");
			strb.append(i+1);
			strb.append(" : ");
			strb.append(ListeDesColonnes.get(i).getNomColonne());
			strb.append("\n");
		}
		return(strb.toString());
		
	}

}
