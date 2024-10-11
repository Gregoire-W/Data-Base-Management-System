package premierPaquet;

import java.util.ArrayList;
import java.util.StringTokenizer;


/**
 * Classe qui verifie si la commande est une jointure
 * 
 * @author Mounir Abdouch, Grégoire woroniak, Socrate Louis Deriza, Olivier Jiang
 * 
 */
public class SelectConditionJoin {
	
	/**
	 * tableau contenant les relations
	 */
	private RelationInfo[] relation;
	
	/**
	 * liste contenant les indices des colonnes de la premiere relation
	 */
	private ArrayList<Integer> indiceColonnesRelation1;
	
	/**
	 * liste contenant les indices des colonnes de la deuxieme relation
	 */
	private ArrayList<Integer> indiceColonnesRelation2;
	
	/**
	 * liste contenant les operateurs
	 */
	private ArrayList<String> operateur;
	
	/**
	 * Construit un select si la commande est un select 
	 * @param commande la commande ecrite par l'utilisateur
	 */
	public SelectConditionJoin (String commande) {
		
		StringTokenizer st1  = new StringTokenizer(commande," ,.<>=");
		int nb1 = st1.countTokens();
		String[] tableauColonneRelation =  new String[nb1];
		for (int i =0; i<nb1; i++) {
			tableauColonneRelation[i] = st1.nextToken();
		}
		
		StringTokenizer st  = new StringTokenizer(commande," ,.");

		int nb = st.countTokens();
		String[] tableauStringCommande = new String[nb];
		for (int i =0; i<nb; i++) {
			tableauStringCommande[i] = st.nextToken();
		}
		
		if(!tableauStringCommande[1].equals("*")){
			System.out.println("Les projections ne sont pas permises dans cette base de données...");
			relation = null;
			indiceColonnesRelation1 = null;
			operateur = null;
			indiceColonnesRelation1 = null;
			return ;
		}
		
		boolean trouve = false;
		int j =0;
		while(j<nb && trouve == false) {
			if(tableauStringCommande[j].equals("FROM")) {
				trouve =true;
			}
			else {
				j++;
			}
			
		}
		if(j== nb) {
			System.out.println("La commande de jointure ne comporte pas de mot clé FROM, elle ne peut aboutir...");
			relation = null;
			indiceColonnesRelation1 = null;
			operateur = null;
			indiceColonnesRelation1 = null;
			return ;
		}
		else {
			
			if((j+2) == nb-1) {
				Catalog catalog = Catalog.getCatalog();
				RelationInfo relation1 = catalog.GetRelationInfo(tableauStringCommande[j+1]);
				RelationInfo relation2 = catalog.GetRelationInfo(tableauStringCommande[j+2]);
				indiceColonnesRelation1 = null;
				operateur = null;
				indiceColonnesRelation1 = null;
				if(relation1!= null && relation2!= null) {
					// Jointure generalise avec des relations existantes
					relation = new RelationInfo[2];
					relation[0] = relation1;
					relation[1] = relation2;
					return ;
				}
				else {
					relation = null;
					return ;
				}
			}
		    else if(!tableauStringCommande[j+3].equals("WHERE")){
				relation = null;
				indiceColonnesRelation1 = null;
				operateur = null;
				indiceColonnesRelation1 = null;
				System.out.println("La commande de jointure prend en paramètre plus 2 relations, ou n'a pas le mot clé WHERE placé correctement, elle ne peut donc pas aboutir...");
				return ;
			}
			else {
				if(j+3 == nb-1) {
					relation = null;
					indiceColonnesRelation1 = null;
					operateur = null;
					indiceColonnesRelation1 = null;
					System.out.println("La commande de jointure ne contient aucune condition, elle ne peut aboutir...");
					return ;
				}
				Catalog catalog = Catalog.getCatalog();
				RelationInfo relation1 = catalog.GetRelationInfo(tableauStringCommande[j+1]);
				RelationInfo relation2 = catalog.GetRelationInfo(tableauStringCommande[j+2]);
				if(relation1!= null && relation2!= null) {
					relation = new RelationInfo[2];
					relation[0] = relation1;
					relation[1] = relation2;
					boolean synthaxeCorrect = true;
					

					int nbStringCritere = ((nb1-1)-(j+3));

					if(nbStringCritere%5 != 4) {
						System.out.println("On constate un problème synthaxique dans les conditions, il y en a trop ou pas assez de paramètre.\nVous avez peut être oublié (ou ajouter en trop) un point,un opérateur, un AND, ou de préciser une colonne ou une relation...");
						synthaxeCorrect = false;
					}
					for(int i = j+4; i<tableauColonneRelation.length; i=i+5) {
						if(!synthaxeCorrect) {
							break;
						}
						if(!tableauColonneRelation[i].equals(relation1.getNomRelation())) {
							System.out.println("On constate un problème synthaxique dans les conditions...\nLa relation "+tableauColonneRelation[i]+" est mal ortographié n'existe pas ou n'est pas à la bonne place");
							synthaxeCorrect =false;
							break;
						}
						if(relation1.getNumeroColonne(tableauColonneRelation[i+1]) == null) {
							synthaxeCorrect =false;
							break;
						}
						if(!tableauColonneRelation[i+2].equals(relation2.getNomRelation())) {
							System.out.println("On constate un problème synthaxique dans les conditions...\nLa relation "+tableauColonneRelation[i+2]+" est mal ortographié, n'existe pas, ou n'est pas à la bonne place");
							synthaxeCorrect =false;
							break;
						}
						if(relation2.getNumeroColonne(tableauColonneRelation[i+3]) == null) {
							synthaxeCorrect =false;
							break;
						}
						
					}
					
					if(synthaxeCorrect) {
						int nombreDeCritere = 1;
						for(int i = j+4; i<tableauColonneRelation.length;i++) {
							if(tableauColonneRelation[i].equals("AND")) {
								nombreDeCritere++;
							}
						}

						if(nombreDeCritere>20) {
							System.out.println("Votre commande de jointure à dépassé le seuil maximum 20 critères, elle ne pourra pas aboutir...");
							synthaxeCorrect = false;
							
						}
					}
					
					if(!synthaxeCorrect) {
						relation = null;
						indiceColonnesRelation1 = null;
						operateur = null;
						indiceColonnesRelation1 = null;
						return ;
					}
					else {
						ArrayList <String> conditionDeSelection = new ArrayList<>();
						ArrayList <String> listeDesOperateursPossible = new ArrayList<>();
						listeDesOperateursPossible.add("<>");
						listeDesOperateursPossible.add("<=");
						listeDesOperateursPossible.add(">=");
						listeDesOperateursPossible.add("<");
						listeDesOperateursPossible.add(">");
						listeDesOperateursPossible.add("=");
						for (int i =j+5; i<nb; i+=4) {
							conditionDeSelection.add(tableauStringCommande[i]);
						}
						operateur = new ArrayList<>();
						
						for(String conditionsSelect: conditionDeSelection) {
							trouve = false;
							for(int i = 0; i<listeDesOperateursPossible.size();i++) {
								if(listeDesOperateursPossible.get(i).length() == 1){
									if(conditionsSelect.contains(listeDesOperateursPossible.get(i))) {
										operateur.add(listeDesOperateursPossible.get(i));
										break;
									}
								}
								else {
									if(conditionsSelect.contains(listeDesOperateursPossible.get(i).subSequence(0, 1)) && conditionsSelect.contains(listeDesOperateursPossible.get(i).subSequence(1, 2))) {
										operateur.add(listeDesOperateursPossible.get(i));
										break;
									}
								}
								
							}
						}
						indiceColonnesRelation1 = new ArrayList<>();
						indiceColonnesRelation2 = new ArrayList<>();
						
						for (int i =j+5; i<nb1; i+=5) {
							
							indiceColonnesRelation1.add(relation1.getNumeroColonne(tableauColonneRelation[i]));
							indiceColonnesRelation2.add(relation2.getNumeroColonne(tableauColonneRelation[i+2]));

						
						}
						
					}
					
				}
				
				
			}
		}
		
	}

	/**
	 * Retourne un tableau contenant les relations
	 * @return retourne les relations
	 */
	public RelationInfo[] getRelation() {
		return relation;
	}
	
    /**
     * Retourne une liste contenant les indices des colonnes de la premiere relation
     * @return une liste contenant les indices des colonnes de la premiere relation
     */
	public ArrayList<Integer> getIndiceColonnesRelation1() {
		return indiceColonnesRelation1;
	}
	
    /**
     * Retourne une liste contenant les indices des colonnes de la deuxieme relation
     * @return une liste contenant les indices des colonnes de la deuxieme relation
     */
	public ArrayList<Integer> getIndiceColonnesRelation2() {
		return indiceColonnesRelation2;
	}
	
    /**
     * Retourne la liste des operateurs
     * @return la liste des operateurs
     */
	public ArrayList<String> getOperateur() {
		return operateur;
	}
    
	
}

