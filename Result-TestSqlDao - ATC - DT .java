import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;

/**
 * Mejorar cada uno de los metodos a nivel SQL y codigo cuando sea necesario
 * Razonar cada una de las mejoras que se han implementado
 * No es necesario que el codigo implementado funcione 
 */
public class TestSqlDao {

	private static final Logger log = LoggerFactory.getLogger(TestSqlDao.class);

	private static TestSqlDao instance = new TestSqlDao();
	private Hashtable<Long, Long> maxOrderUser;

	private Connection connection;
	
	private TestSqlDao() {

	}

	private static TestSqlDao getInstance() {

		return instance;
	}

	/**
	 * Obtiene el ID del ultimo pedido para cada usuario
	 */
	public Hashtable<Long, Long> getMaxUserOrderId(long idTienda) throws Exception {

		String query = "SELECT ID_PEDIDO, ID_USUARIO FROM PEDIDOS WHERE ID_TIENDA = ?";

		try (PreparedStatement stmt = getConnection().prepareStatement(query)) {
			stmt.setLong(1, idTienda);
			ResultSet rs = stmt.executeQuery();

			maxOrderUser = new Hashtable<Long, Long>();

			while (rs.next()) {

				long idPedido = rs.getInt("ID_PEDIDO");
				long idUsuario = rs.getInt("ID_USUARIO");

				if (!maxOrderUser.containsKey(idUsuario)) {

					maxOrderUser.put(idUsuario, idPedido);

				} else if (maxOrderUser.get(idUsuario) < idPedido) {

					maxOrderUser.put(idUsuario, idPedido);
				}
			}

		} catch (SQLException e) {
			log.info("Producto no encontrado con id : {}", idTienda);
		}

		return maxOrderUser;
	}

	/**
	 * Copia todos los pedidos de un usuario a otro
	 */
	public void copyUserOrders(long idUserOri, long idUserDes) throws Exception {

		String query = "SELECT FECHA, TOTAL, SUBTOTAL, DIRECCION FROM PEDIDOS WHERE ID_USUARIO = ?";

		try (PreparedStatement stmt = getConnection().prepareStatement(query)) {
			getConnection().setAutoCommit(false);

			stmt.setLong(1, idUserOri);
			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				String insert = "INSERT INTO PEDIDOS (FECHA, TOTAL, SUBTOTAL, DIRECCION) VALUES (?, ?, ?, ?)";

				PreparedStatement stmt2 = getConnection().prepareStatement(insert);
				stmt2.setDate(1, "FECHA");
				stmt2.setDouble(2, "TOTAL");
				stmt2.setDouble(3, "SUBTOTAL");
				stmt2.setString(4, "DIRECCION");

				stmt2.executeUpdate();
			}
			getConnection().commit();


		} catch (SQLException e) {
			log.info("Pedido no encontrado con id : {}", idUserOri);
		}
	}

	/**
	 * Obtiene los datos del usuario y pedido con el pedido de mayor importe para la tienda dada
	 */
	public void getUserMaxOrder(long idTienda) throws Exception {

		String query = "SELECT U.ID_USUARIO, P.ID_PEDIDO, P.TOTAL, U.NOMBRE, U.DIRECCION FROM PEDIDOS AS P "
				+ "INNER JOIN USUARIOS AS U ON P.ID_USUARIO = U.ID_USUARIO WHERE P.ID_TIENDA = ?";

		try (PreparedStatement stmt = getConnection().prepareStatement(query)) {
			stmt.setLong(1, idTienda);
			ResultSet rs = stmt.executeQuery();
			double total = 0;

			while (rs.next()) {

				if (rs.getLong("TOTAL") > total) {

					double total = rs.getDouble("TOTAL");
					long userId = rs.getLong("ID_USUARIO");
					long orderId = rs.getLong("ID_PEDIDO");
					String name = rs.getString("NOMBRE");
					String address = rs.getString("DIRECCION");
				}
			}
		}
	}

	private Connection getConnection() throws SQLException {

		if(connection == null) {
			connection = getConnection();
		}
		// return JDBC connection
		return connection;
	}
}
