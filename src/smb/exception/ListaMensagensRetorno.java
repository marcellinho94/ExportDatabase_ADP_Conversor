package smb.exception;

import java.util.ArrayList;

/**
 * 
 * Classe responsavel por armazenas as mensagens de retorno a aplicacao.
 * 
 * @author Wesley Camilo
 * 
 */
public class ListaMensagensRetorno extends ArrayList<MsgRetorno> {

	private static final long serialVersionUID = -2175397573172045321L;

	/**
	 * Adiciona uma mensagem generica a lista de mensagens.
	 * 
	 * @param descricaoMensagem Descricao da mensagem.
	 * @param localOcorrencia   Local de ocorrencia da mensagem.
	 * @param tipoMensagem      Tipo da mensagem.
	 * @return true para o caso de sucesso na operacao e false para o caso
	 *         contrario.
	 */
	public boolean add(String descricaoMensagem, String localOcorrencia, String tipoMensagem) {
		if (tipoMensagem.equals(MsgRetorno.RETORNO_ERRO)) {
		} else if (tipoMensagem.equals(MsgRetorno.RETORNO_ERRO_SISTEMA)) {
		}
		return super.add(new MsgRetorno(descricaoMensagem, localOcorrencia, tipoMensagem));
	}

	/**
	 * Adiciona uma mensagem de erro a lista de mensagens.
	 * 
	 * @param descricaoMensagem Descricao da mensagem.
	 * @param localOcorrencia   Local de ocorrencia da mensagem.
	 * @return true para o caso de sucesso na operacao e false para o caso
	 *         contrario.
	 */
	public boolean addErro(String descricaoMensagem, String localOcorrencia) {
		return this.add(descricaoMensagem, localOcorrencia, MsgRetorno.RETORNO_ERRO);
	}

	/**
	 * Adiciona uma mensagem de alerta a lista de mensagens.
	 * 
	 * @param descricaoMensagem Descricao da mensagem.
	 * @param localOcorrencia   Local de ocorrencia da mensagem.
	 * @return true para o caso de sucesso na operacao e false para o caso
	 *         contrario.
	 */
	public boolean addAlerta(String descricaoMensagem, String localOcorrencia) {
		// log.warn(localOcorrencia + ": " + descricaoMensagem);
		return this.add(descricaoMensagem, localOcorrencia, MsgRetorno.RETORNO_ALERTA);
	}

	/**
	 * Adiciona uma mensagem de informacao a lista de mensagens.
	 * 
	 * @param descricaoMensagem Descricao da mensagem.
	 * @param localOcorrencia   Local de ocorrencia da mensagem.
	 * @return true para o caso de sucesso na operacao e false para o caso
	 *         contrario.
	 */
	public boolean addSucesso(String descricaoMensagem, String localOcorrencia) {
		// log.info(localOcorrencia + ": " + descricaoMensagem);
		return this.add(descricaoMensagem, localOcorrencia, MsgRetorno.RETORNO_INFO);
	}

	/**
	 * Verifica a existencia de mensagens de erro na lista de mensagens.
	 * 
	 * @return true caos haja mensagens de erro na lista de mensagens e false para o
	 *         caso contrario.
	 */
	public boolean temErros() {
		boolean result = false;

		if (super.size() != 0) {
			for (int i = 0; i < super.size(); i++) {
				if (super.get(i).getTipoMensagem() == MsgRetorno.RETORNO_ERRO) {
					result = true;
					break;
				}
			}
		}
		return result;
	}

	/**
	 * Verifica a existencia de mensagens de alerta na lista de mensagens.
	 * 
	 * @return true caos haja mensagens de erro na lista de mensagens e false para o
	 *         caso contrario.
	 */
	public boolean temAlertas() {
		boolean result = false;

		if (super.size() != 0) {
			for (int i = 0; i < super.size(); i++) {
				if (super.get(i).getTipoMensagem() == MsgRetorno.RETORNO_ALERTA) {
					result = true;
					break;
				}
			}
		}
		return result;
	}

	/**
	 * Converte a lista de mensagens para um formato de texto.
	 */
	public String toString() {
		StringBuffer msgs = new StringBuffer("");
		for (int i = 0; i < super.size(); i++) {
			msgs.append(super.get(i).toString() + "\n");
		}

		return msgs.toString();
	}

}
