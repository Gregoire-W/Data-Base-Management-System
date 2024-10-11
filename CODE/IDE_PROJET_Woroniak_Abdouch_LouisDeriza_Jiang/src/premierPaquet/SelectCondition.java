package premierPaquet;

import java.util.ArrayList;
import java.util.StringTokenizer;
/**
 * Classe verifie si la commande est un select
 * 
 * @author Mounir Abdouch, Grégoire woroniak, Socrate Louis Deriza, Olivier Jiang
 *
 */

public class SelectCondition {
	/**
	 * la relation ou l'on cherche les records
	 */
    private RelationInfo relation;
    
    /**
     * liste contenant les indices des colonnes
     */
    private ArrayList<Integer> indiceColonnes;
    
    /**
     * liste contenant les operateurs
     */
    private ArrayList<String> operateur;
    
    /**
     * liste contenant les valeurs de comparaison
     */
    private ArrayList<String> valeurDeComparaison;
    
    
    /**
     * Verifie si la commande est un select
     * 
     * @param commande la commande ecrite par l'utilisateur
     */
    public SelectCondition (String commande) {
    	
    	StringTokenizer st  = new StringTokenizer(commande," ");
		String nomRelation;
		int nb = st.countTokens();
		String[] tableauStringCommande = new String[nb];
		for (int i =0; i<nb; i++) {
			tableauStringCommande[i] = st.nextToken();
		}
		if(!tableauStringCommande[1].equals("*")){
			System.out.println("Les projections ne sont pas permises dans cette base de données...");
			relation = null;
			indiceColonnes = null;
			operateur = null;
			valeurDeComparaison = null;
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
			System.out.println("La commande select ne comporte pas de mot clé FROM, elle ne peut aboutir...");
			relation = null;
			indiceColonnes = null;
			operateur = null;
			valeurDeComparaison = null;
			return ;
		}
		else {
			
			nomRelation = tableauStringCommande[j+1];
			Catalog catalog = Catalog.getCatalog();
			relation = catalog.GetRelationInfo(nomRelation);
			
			if((j+1) == (nb-1)) {
				// cas du select general
				indiceColonnes = null;
				operateur = null;
				valeurDeComparaison = null;
				return ;
			}
			else {
				
				if(relation != null) {
					trouve = false;
					while(j<nb && trouve == false) {
						if(tableauStringCommande[j].equals("WHERE")) {
							trouve =true;
						}
						else {
							j++;
						}
						
					}
					if(j!= nb) {
						if(j == nb-1) {
							System.out.println("Il n'y a aucun critère de condition après le mot clé 'WHERE'...");
							System.out.println("Votre commande de selection ne respectant pas les normes du langage sql, elle ne pourra pas aboutir...");
							relation = null;
							indiceColonnes = null;
							operateur = null;
							valeurDeComparaison = null;
							return ;
						}
						ArrayList <String> conditionDeSelection = new ArrayList<>();
						ArrayList<String> elementDesComparaison;
						ArrayList <String> listeDesOperateursPossible = new ArrayList<>();
						listeDesOperateursPossible.add("<>");
						listeDesOperateursPossible.add("<=");
						listeDesOperateursPossible.add(">=");
						listeDesOperateursPossible.add("<");
						listeDesOperateursPossible.add(">");
						listeDesOperateursPossible.add("=");
						for (int i =j+1; i<nb; i+=2) {
							conditionDeSelection.add(tableauStringCommande[i]);
						}
						if(conditionDeSelection.size()>20) {
							System.out.println("Votre commande de selection à dépassé le seuil maximum 20 critères, elle ne pourra pas aboutir...");
							relation = null;
							indiceColonnes = null;
							operateur = null;
							valeurDeComparaison = null;
							return ;
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
						StringTokenizer st1;
						elementDesComparaison = new ArrayList<>();
						for(int i = 0; i<conditionDeSelection.size(); i++) {
							st1 = new StringTokenizer(conditionDeSelection.get(i),operateur.get(i));
							while(st1.hasMoreElements()) {
								elementDesComparaison.add(st1.nextToken());
							}
						}
						boolean uneColonneExistePas = false;
						valeurDeComparaison = new ArrayList<>();
						indiceColonnes = new ArrayList<>();
						for(int i =1; i<elementDesComparaison.size(); i+=2) {
							if(relation.getNumeroColonne(elementDesComparaison.get(i-1))!=null) {
								indiceColonnes.add(relation.getNumeroColonne(elementDesComparaison.get(i-1)));
								valeurDeComparaison.add(elementDesComparaison.get(i));
							}
							else {
								uneColonneExistePas = true;
								break;
							}
						}
						if(uneColonneExistePas) {
							
							relation = null;
							indiceColonnes = null;
							operateur = null;
							valeurDeComparaison = null;
						}
						
						
					}
					else {
						relation = null;
						indiceColonnes = null;
						operateur = null;
						valeurDeComparaison = null;
						System.out.println("Après le nom de la relation: "+nomRelation+", nous ne parvenons pas à trouver de mot clé 'WHERE'...");
						System.out.println("Votre commande de selection ne respectant pas les normes du langage sql, elle ne pourra pas aboutir...");
					}
					
				}
			}
			
	    }
			
    }
    /**
     * Gere la commande select connaissant la relation, les indices des colonnes, les operateurs et les valeurs de comparaison
     * 
     * @param relation la relation sur laquelle on souhaite faire le select
     * @param indiceColonnes liste des indices des colonnes
     * @param operateur liste des operateurs 
     * @param valeurDeComparaison liste des valeurs de comparaison
     */

    public SelectCondition(RelationInfo relation,ArrayList<Integer> indiceColonnes, ArrayList<String> operateur, ArrayList<String> valeurDeComparaison) {
    	this.relation = relation;
    	this.indiceColonnes = indiceColonnes;
    	this.operateur = operateur;
    	this.valeurDeComparaison = valeurDeComparaison;
    	
    }
    
    /**
     * Gere la commande select avec uniquement la relation 
     * @param relation la relation sur laquelle ou souhaite faire le select
     */
    public SelectCondition(RelationInfo relation) {
    	this.relation = relation;
    	this.indiceColonnes = null;
    	this.operateur = null;
    	this.valeurDeComparaison = null;
    	
    }
    
    /**
     * Retourne la relation 
     * @return une relation
     */
    public RelationInfo getRelation() {
		return relation;
	}

	
    /**
     * Retourne l'indice des colonnes
     * @return une liste contenant les indices des colonnes
     */
	public ArrayList<Integer> getIndiceColonnes() {
		return indiceColonnes;
	}

	/**
	 * Retourne une liste des operateurs de la commande
	 * @return la liste des operateurs
	 */
	public ArrayList<String> getOperateur() {
		return operateur;
	}

	/**
	 * Permet de retourner la liste valeurDeComparaison
	 * @return la liste des valeurs de comparaison
	 */
	public ArrayList<String> getValeurDeComparaison() {
		return valeurDeComparaison;
	}

	
    
    
}
