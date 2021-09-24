package smb.utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import smb.exception.MsgRetorno;
import smb.exception.PadraoException;

public class ConexaoDiretaBanco {

	private String usuario;
	private String senha;
	private String strConexao;
	private Connection conexao;

	public ConexaoDiretaBanco(String server, String database, String user, String password)
			throws PadraoException, IOException {
		super();

		usuario = user;
		senha = password;
		String door = "1433";
		String instance = "";

		if (Utilitario.StringNullOuVaziaComTrim(server) || Utilitario.StringNullOuVaziaComTrim(database)
				|| Utilitario.StringNullOuVaziaComTrim(usuario) || Utilitario.StringNullOuVaziaComTrim(senha)
				|| Utilitario.StringNullOuVaziaComTrim(door)) {

			MsgRetorno m = new MsgRetorno("Parametros do servidor não configurados.", MsgRetorno.RETORNO_ALERTA);
			throw new PadraoException(m);
		}

		strConexao = "jdbc:jtds:sqlserver://" + server + ":" + door + "/" + database;

		if (!Utilitario.StringNullOuVaziaComTrim(instance)) {
			strConexao += ";instance=" + instance;
		}
	}

	public void conectar() throws PadraoException {
		if (conexao != null)
			return;

		try {
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			conexao = DriverManager.getConnection(strConexao, usuario, senha);

		} catch (Exception e) {
			throw new PadraoException(e);
		}
	}

	public void desconectar() throws PadraoException {
		try {
			if (conexao == null)
				return;

			conexao.close();
			conexao = null;

		} catch (Exception e) {
			throw new PadraoException(e);
		}
	}

	public List<Object[]> obterTodosComSqlLivre(String sql, boolean conectarDesconectarAutomatico)
			throws PadraoException {

		if (conexao == null) {
			if (conectarDesconectarAutomatico) {
				conectar();
			} else {
				MsgRetorno m = new MsgRetorno("Não conectado.", MsgRetorno.RETORNO_ALERTA);
				throw new PadraoException(m);
			}
		}

		List<Object[]> retorno = null;

		Statement st = null;
		ResultSet rs = null;
		try {

			st = conexao.createStatement();
			rs = st.executeQuery(sql);

			if (rs == null) {
				if (conectarDesconectarAutomatico)
					desconectar();

				return null;
			}

			retorno = new ArrayList<Object[]>();
			ResultSetMetaData metadata = rs.getMetaData();
			int numCols = metadata.getColumnCount();

			while (rs.next()) {
				Object[] row = new Object[numCols];
				for (int i = 0; i < numCols; i++) {
					row[i] = rs.getObject(i + 1);
				}

				retorno.add(row);
			}
			rs.close();

			if (conectarDesconectarAutomatico)
				desconectar();

			return retorno;

		} catch (Exception e) {
			try {
				if (rs != null && !rs.isClosed()) {
					rs.close();
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			}

			if (conectarDesconectarAutomatico)
				desconectar();

			throw new PadraoException(e);

		}

	}

	public void executaSql(String sql, boolean conectarDesconectarAutomatico) throws PadraoException {

		if (conexao == null) {
			if (conectarDesconectarAutomatico) {
				conectar();
			} else {
				MsgRetorno m = new MsgRetorno("NÃ£o conectado.", MsgRetorno.RETORNO_ALERTA);
				throw new PadraoException(m);
			}
		}

		try {
			PreparedStatement pstmt = conexao.prepareStatement(sql);
			pstmt.execute();

			if (conectarDesconectarAutomatico)
				desconectar();

		} catch (Exception e) {
			if (conectarDesconectarAutomatico)
				desconectar();

			throw new PadraoException(e);
		}
	}
}
