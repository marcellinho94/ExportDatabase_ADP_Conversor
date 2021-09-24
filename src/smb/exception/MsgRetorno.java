package smb.exception;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 
 * Mantem o padrao das mensagens a serem retornadas a aplicacao flex.
 * 
 * @author Wesley Camilo
 * @version 1.0
 * 
 */
public class MsgRetorno {

	public final static String RETORNO_ALERTA = "[Alerta]";
	public final static String RETORNO_ERRO = "[Erro]";
	public final static String RETORNO_INFO = "[Info]";
	public final static String RETORNO_ERRO_SISTEMA = "[Erro_Sistema]";
	public final static String MSG_PADRAO = "Ocorreu um erro durante a operação. Informe à coordenadoria de informática.";

	private String descricaoMensagem;
	private String localOcorrencia;
	private String tipoMensagem;

	/**
	 * Construtor.
	 * 
	 * @param descricaoMensagem Descricao da mensagem.
	 * @param localOcorrencia   Local de ocorrencia da mensagem.
	 * @param tipoMensagem      Tipo da mensagem.
	 */
	public MsgRetorno(String descricaoMensagem, String localOcorrencia, String tipoMensagem) {
		super();
		this.descricaoMensagem = descricaoMensagem;
		this.localOcorrencia = localOcorrencia;
		setTipoMensagem(tipoMensagem);
		if (this.getTipoMensagem().equals(RETORNO_ERRO)) {
			Logger.getLogger(MsgRetorno.class.getName()).log(Level.SEVERE, descricaoMensagem + ":" + localOcorrencia);
		} else if (this.getTipoMensagem().equals(RETORNO_ERRO_SISTEMA)) {
			Logger.getLogger(MsgRetorno.class.getName()).log(Level.SEVERE, descricaoMensagem + ":" + localOcorrencia);
		}
	}

	public MsgRetorno(String descricaoMensagem, String tipoMensagem) {
		super();
		this.descricaoMensagem = descricaoMensagem;
		setTipoMensagem(tipoMensagem);
		if (this.getTipoMensagem().equals(RETORNO_ERRO)) {
			Logger.getLogger(MsgRetorno.class.getName()).log(Level.SEVERE, descricaoMensagem);
		} else if (this.getTipoMensagem().equals(RETORNO_ERRO_SISTEMA)) {
			Logger.getLogger(MsgRetorno.class.getName()).log(Level.SEVERE, descricaoMensagem);
		}
	}

	public MsgRetorno(String descricaoMensagem) {
		super();
		this.descricaoMensagem = descricaoMensagem;
		setTipoMensagem(RETORNO_ERRO);
	}

	/**
	 * Obtem a descricao da mensagem
	 * 
	 * @return Descricao da mensagem.
	 */
	public String getDescricaoMensagem() {
		return descricaoMensagem;
	}

	/**
	 * Configura a descricao da mensagem.
	 * 
	 * @param descricaoMensagem Descricao a ser configurada na mensagem.
	 */
	public void setDescricaoMensagem(String descricaoMensagem) {
		this.descricaoMensagem = descricaoMensagem;
	}

	/**
	 * Obtem o nome da classe onde o erro ocorreu.
	 * 
	 * @return O nome da classe onde o erro ocorreu.
	 */
	public String getLocalOcorrencia() {
		return localOcorrencia;
	}

	/**
	 * Configura o nome da classe onde o erro ocorreu.
	 * 
	 * @param LocalOcorrencia Local de ocorrencia a ser configurado na mensagem.
	 */
	public void setLocalOcorrencia(String LocalOcorrencia) {
		this.localOcorrencia = LocalOcorrencia;
	}

	/**
	 * Obtem o tipo da mensagem.
	 * 
	 * @return O tipo da mensagem, podendo ela ser de informacao, alerta ou erro.
	 */
	public String getTipoMensagem() {
		if (tipoMensagem == null) {
			tipoMensagem = RETORNO_ERRO;
		}
		return tipoMensagem;
	}

	/**
	 * Configura o tipo da mensagem.
	 * 
	 * @param tipoMensagem Tipo da mensagem a ser configurado.
	 */
	public void setTipoMensagem(String tipoMensagem) {
		if (!tipoMensagem.equals(RETORNO_ALERTA) && !tipoMensagem.equals(RETORNO_ERRO)
				&& !tipoMensagem.equals(RETORNO_INFO) && !tipoMensagem.equals(RETORNO_ERRO_SISTEMA)) {
			this.tipoMensagem = RETORNO_ERRO;
		} else {
			this.tipoMensagem = tipoMensagem;
		}

	}

	/**
	 * Converte a mensagem em um formato de texto.
	 */
	@Override
	public String toString() {
		return getTipoMensagem() + " " + getDescricaoMensagem();
	}

}
