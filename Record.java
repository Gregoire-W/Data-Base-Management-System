package premierPaquet;
import java.lang.String;

import java.nio.ByteBuffer;

import java.util.ArrayList;

import java.util.StringTokenizer;

public class Record {
	private RelationInfo relInfo;
	ArrayList <String> values;
	ArrayList<Integer> offSet;	
	public Record(RelationInfo rI, ArrayList <String> v) {
		if(v.size() == rI.nombreDeColonne()) {
			relInfo = rI;
			values = v;
			offSet = new ArrayList<Integer>();
		}
		else {
			System.out.println("Cette instance n'a pas le bon nombre d'entrée necessaire pour la relation '"+rI.getNomRelation()+"'...");
			System.out.println("Nombre de valeurs: "+v.size()+", Nombre de colonne dans la relation: "+rI.nombreDeColonne());
		}
    }
	public Record(RelationInfo rI,ArrayList<Integer> oS, ArrayList <String> v) {
		if(v.size() == rI.nombreDeColonne()) {
			relInfo = rI;
			values = v;
			offSet = oS;
			
			

		}
    }

	public void writeToBuffer(ByteBuffer buff,int pos) {
		StringTokenizer st ;
		String numeroString;
		int positionValues = pos + 4*(values.size()+1);
		this.offSet.add(positionValues);
		for(int i =0;i< values.size();i++) {
			
			if(relInfo.getListeDesColonnes().get(i).getType().equals("INTEGER") == true) {
		
					offSet.add(offSet.get(i)+4);
				
			}
			else if(relInfo.getListeDesColonnes().get(i).getType().equals("REAL") == true) {
				
					offSet.add(offSet.get(i)+4);
				
				
			}
			else {
				st = new StringTokenizer(relInfo.getListeDesColonnes().get(i).getType(),"()");
				st.nextElement();
				numeroString = st.nextElement().toString();
				if(values.get(i).length()<=Integer.parseInt(numeroString)) {
					
						offSet.add(offSet.get(i)+(2*(values.get(i).length())));
					    
					
					
				}
				else {
					System.out.println("Attention votre chaine de caractère: '"+values.get(i)+"' dépasse les "+Integer.parseInt(numeroString)+"autorisé.");
					System.out.println("Chaîne de caracère non écrit...");
				}
			}
			
		}
		
		for(int i =0;i< offSet.size();i++){
			buff.putInt(pos, offSet.get(i));
			pos+=4;
		}
		
		for(int i =0;i< values.size();i++) {
			if(relInfo.getListeDesColonnes().get(i).getType().equals("INTEGER") == true) {
				buff.putInt(offSet.get(i), Integer.parseInt(values.get(i)));
			}
			else if(relInfo.getListeDesColonnes().get(i).getType().equals("REAL") == true) {
				buff.putFloat(offSet.get(i), Float.parseFloat(values.get(i)));
			}
			else {
				for(int k = 0;k < values.get(i).length();k++) {
					buff.putChar(offSet.get(i) + 2*k, values.get(i).charAt(k));
				}
			}
		}

		
	}
	
	public void readFrombuffer(ByteBuffer buff,int pos) {
		//ByteBuffer readByteBuffer;

		int taille;
		System.out.println("Valeurs du offset (relative à la page! ): ");
		for(int v = pos;v < buff.getInt(pos);v = v+4) {
			System.out.print(buff.getInt(v)+" ");
		}
		System.out.println();
		System.out.println();
		
		for(int i =0;i< values.size();i++) {
			System.out.print(relInfo.getListeDesColonnes().get(i).getNomColonne()+" : ");
			if(relInfo.getListeDesColonnes().get(i).getType().equals("INTEGER") == true) {
				System.out.println(buff.getInt(buff.getInt(pos)));
				pos +=4;
			}
			else if(relInfo.getListeDesColonnes().get(i).getType().equals("REAL") == true) {
				System.out.println(buff.getFloat(buff.getInt(pos)));
				pos +=4;
			}
			else {
				taille = buff.getInt(pos+4)-buff.getInt(pos);
				for(int k =  0;k< taille;k = k+2) {
					//System.out.print(k+"+");
					System.out.print(buff.getChar((buff.getInt(pos)+ k)));

					
				}
				System.out.println();
				
				pos+=4;
				
			}
		}
		System.out.println();
	}	
	public static ArrayList<Integer> readFromBufferOffset(ByteBuffer buff, int pos) {
		ArrayList<Integer> offset = new ArrayList<Integer>();
		for(int v = pos; v < buff.getInt(pos);v = v+4) {
			offset.add(buff.getInt(v));
		}
		return(offset);
	}
	public static ArrayList<String> readFromBufferValues(RelationInfo rI, ByteBuffer buff, int pos) {
		ArrayList<String> data = new ArrayList<>();
		for(int i =0;i< rI.nombreDeColonne();i++) {
			if(rI.getListeDesColonnes().get(i).getType().equals("INTEGER") == true) {
				int e = buff.getInt(buff.getInt(pos));
				data.add(Integer.toString(e));
				pos +=4;
			}
			else if(rI.getListeDesColonnes().get(i).getType().equals("REAL") == true) {
				float r = buff.getFloat(buff.getInt(pos));
				data.add(Float.toString(r));
				pos+=4;
			}
			else {
				int taille = buff.getInt(pos+4)-buff.getInt(pos);
				StringBuilder strb = new StringBuilder("");
				for(int k =  0;k< taille;k = k+2) {
					char s = buff.getChar((buff.getInt(pos)+ k));
					strb.append(s);
				}
				data.add(strb.toString());
				pos+=4;
				
			}
			
		}
		return(data);
		
		
	}
	public int getWrittenSize() {
		int taille = 4*(this.relInfo.nombreDeColonne() + 1);
		for(int i = 0; i < values.size(); i++) {
			if(relInfo.getListeDesColonnes().get(i).getType().equals("INTEGER")) {
				taille += 4;
			}
			else if(relInfo.getListeDesColonnes().get(i).getType().equals("REAL")) {
				taille += 4;
			}
			else {
				taille += (2*values.get(i).length());
			}
		}
		return taille;
	}

	public RelationInfo getRelInfo() {
		return relInfo;
	}
      
	public void setOffSet(ArrayList<Integer> offSet) {
		this.offSet = offSet;
	}

	public String toString2() {
		StringBuilder strb = new StringBuilder("");
		if(!this.offSet.isEmpty()) {
			strb.append("Valeurs du offset du record: ");
			for(int v = 0;v < this.offSet.size();v++) {
				strb.append(this.offSet.get(v)+ " ");
			}
			strb.append("\n");
		}
		for(int i =0;i< values.size();i++) {
			strb.append(relInfo.getListeDesColonnes().get(i).getNomColonne()+" : ");
			strb.append(this.values.get(i));
			strb.append("\n");
		}
		return(strb.toString());
	}
    

	public String toString() {
		String espacePointVirguleEspace = " ; ";

		StringBuilder strb = new StringBuilder(this.values.get(0));
		for(int i  = 1 ; i<this.values.size(); i++) {
			strb.append(espacePointVirguleEspace);
			strb.append(this.values.get(i));
		}
		return(strb.toString());
	}
	public ArrayList<String> getValues() {
		return values;
	}

	public ArrayList<Integer> getOffset() {
		return offSet;
	}
	

}
