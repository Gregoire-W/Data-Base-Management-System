package premierPaquet;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.LocalTime;
/**
 * Represente les différentes caractéristiques d'une case du BufferManager
 * 
 * @author Mounir Abdouch, Grégoire woroniak, Socrate Louis Deriza, Olivier Jiang
 *
 */
public class Frame implements Serializable{
	
	/**
	 * numero associe a notre classe serialisable
	 */
	private static final long serialVersionUID = -2825342018408868052L;
	
	/*
	 * le Buffer de la case, non sérialisable
	 */
	private transient ByteBuffer  buffer;
	
	/*
	 * la page associé à la case
	 */
	private PageId page;
	
	/*
	 * le nombre d'utilisateur actuelle de la case
	 */
	private int pinCount;
	/*
	 * l'indicateur de modification de la case
	 */
	private boolean flagDirty;
	
	/*
	 * le dernière fois où le nombre d'utilisateur de la case est passé à zéro
	 */
	private LocalTime tempsPinCountPasseAZero;
	
	/**
	 * 
	 * Construit et initialise une case
	 * 
	 */
	public Frame() {
		buffer = ByteBuffer.allocate(DBParams.PageSize);
		page = null;
		pinCount = 0;
		flagDirty =false;
		tempsPinCountPasseAZero = null;
	}
	
	/**
	 * 
	 * Permet d'obtenir le Buffer de la case
	 * 
	 * @return le Buffer de la case
	 */
	public ByteBuffer getBuffer(){
		return (buffer);
	}
	
	/**
	 * 
	 * Permet d'obtenir la page associé à la case
	 * 
	 * @return la page associé à la case
	 */
	public PageId getPage(){
		return (page);
	}
	
	/**
	 * 
	 * Permet d'obtenir le nombre d'utilisateur actuelle de la case
	 * 
	 * @return le nombre d'utilisateur actuelle de la case
	 */
	public int getPinCount(){
		return (pinCount);
	}
	
	/**
	 * 
	 * Permet d'obtenir l'indicateur de modification de la case
	 * 
	 * @return l'indicateur de modification de la case
	 */
	public boolean getFlagDirty(){
		return (flagDirty);
	}
	
	/**
	 * Permet de modifier la page associé à la case
	 * 
	 * @param nouvellePage la nouvelle page associé à la case
	 */
	public void setPage(PageId nouvellePage) {
		page = nouvellePage;
	}
	
	/*
	 * Permet de nettoyer/rénitialiser le Buffer de la case
	 */
	public void clearBuffer() {
		buffer = ByteBuffer.allocate(DBParams.PageSize);
	}
	
	/**
	 * Permet de modifier l'indicateur de modification de la case
	 * 
	 * @param b le nouvel indicateur de modification de la case
	 */
	public void setFlagDirty(boolean b) {
		flagDirty =b;
		
	}
	
	/**
	 * Permet de remplacer le Buffer de la case
	 * 
	 * @param buff le buffer par lequel on veut remplacer l'ancien buffer
	 */
	public void setBuffer(ByteBuffer buff) {
		buffer = buff;
	}
	
	/*
	 * Permet d'incrémenter le nombre d'utilisateur actuelle de la case
	 */
	public void incrementerPinCount() {
		pinCount++;
	}
	
	/*
	 * Permet de décrémenter le nombre d'utilisateur actuelle de la case
	 */
	public void decrementerPinCount() {
		pinCount--;
	}
	
	/*
	 * Permet de re-initialiser le nombre d'utilisateur actuelle de la case à zéro
	 */
	public void reInitialiserPinCount() {
		pinCount = 0;
	}
	
	/*
	 * Permet de re-initialiser l'indicateur de modification de la case à false
	 */
    public void reInitialiserFlagDirty() {
    	flagDirty =false;
	}
    
    /*
	 * Permet de re-initialiser le Buffer de la case
	 */
    public void reInitialiserBuffer() {
        buffer.rewind();
    }
    
    /**
	 * 
	 * Permet d'obtenir le temps correspondant à la dernière fois où le nombre d'utilisateur de la case est passé à zéro
	 * 
	 * @return le dernière fois où le nombre d'utilisateur de la case est passé à zéro
	 */
    public LocalTime getTime() {
    	return (tempsPinCountPasseAZero);
    }
    
    /**
	 * Permet de modifier le temps correspondant à la dernière fois où le nombre d'utilisateur de la case est passé à zéro
	 * 
	 * @param nouveauTemps le nouveau temps 
	 */
    public void setTime(LocalTime nouveauTemps) {
    	tempsPinCountPasseAZero = nouveauTemps;
    }
}
