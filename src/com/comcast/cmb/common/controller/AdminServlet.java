/**
 * Copyright 2012 Comcast Corporation
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.comcast.cmb.common.controller;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.PersistenceException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;

/**
 * Administrator application.
 *
 * @author bwolf, aseem, baosen, tina, michael
 */
public class AdminServlet extends AdminServletBase {

  private static final long serialVersionUID = 1L;
  private static Logger logger = Logger.getLogger(AdminServlet.class);

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    if (redirectNonAdminUser(request, response)) {
      return;
    }

    CMBControllerServlet.valueAccumulator.initializeAllCounters();
    response.setContentType("text/html");
    PrintWriter out = response.getWriter();
    Map<?, ?> parameters = request.getParameterMap();
    String userName = request.getParameter("user");
    String passwd = request.getParameter("password");
    String isAdminStr = request.getParameter("isAdmin");
    String description = request.getParameter("description") + "";
    IUserPersistence userHandler = PersistenceFactory.getUserPersistence();

    if (parameters.containsKey("Create")) {

      try {
        if (userHandler.getUserByName(userName) != null) {
          out.println("<p><i>User already exists!</i></p>");
          logger.debug("event=user_already_exists user_name=" + userName);
        } else {
          Boolean isAdmin = Boolean.parseBoolean(isAdminStr);
          userHandler.createUser(userName, passwd, isAdmin, description);
          logger.debug("event=create_user user_name=" + userName);
        }
      } catch (PersistenceException ex) {
        logger.error("event=create_user user_name=" + userName, ex);
        throw new ServletException(ex);
      }

    } else if (parameters.containsKey("Delete")) {

      try {
        userHandler.deleteUser(userName);
        logger.debug("event=delete_user user_name=" + userName);
      } catch (PersistenceException ex) {
        logger.error("event=delete_user user_name=" + userName, ex);
        throw new ServletException(ex);
      }
    }

    out.println("<html>");

    header(request, out, "All Users");

    out.println("<body>");

    out.println("<h2>All Users</h2>");
    out.print("<form action=\"/webui\" method=POST>");
    out.print("<table><tr><td>Username:</td><td>Password:</td><td>Description:</td><td></td></tr>");
    out.print(
        "<tr>"
            + "<td><input type='text' name='user'/></td>"
            + "<td><input type='password' name='password'></td>"
            + "<td><input type='text' name='description'/></td>"
            + "<td><input type='checkbox' id='isAdmin' name='isAdmin' value='true'><label for='isAdmin'>Is Admin</label></td>"
            + "<td><input type='submit' value='Create' name='Create' /></td></tr>");
    out.print("</table>");
    out.print("</form>");
    List<User> users = new ArrayList<User>();

    try {

      users = userHandler.getAllUsers();
      Collections.sort(
          users,
          new Comparator() {
            public int compare(Object o1, Object o2) {
              User u1 = (User) o1;
              User u2 = (User) o2;
              return u1.getUserName().compareToIgnoreCase(u2.getUserName());
            }
          });

    } catch (PersistenceException ex) {
      logger.error("event=get_all_users", ex);
      throw new ServletException(ex);
    }

    for (int i = 0; users != null && i < users.size(); i++) {

      if (i == 0) {
        out.println("<p><hr width='80%' align='left' /><p>");
        out.println("<table  class='alternatecolortable' border='1' width='80%' >");
        out.println("<tr><th>User Name</th>");
        out.println("<th>User ID</th>");
        out.println("<th>Is Admin</th>");
        out.println("<th>Access Key</th>");
        out.println("<th>Access Secret</th><th>&nbsp;</th><th>&nbsp;</th><th>&nbsp;</th></tr>");
      }

      User user = (User) users.get(i);

      out.println("<tr>");
      out.println("<form action=\"/" + response.encodeURL("webui") + "\" method=POST>");
      out.println(
          "<td>"
              + user.getUserName()
              + "<input type='hidden' name='user' value="
              + user.getUserName()
              + ">"
              + (user.getDescription().isEmpty() ? "" : "<br/><i>" + user.getDescription() + "</i>")
              + "</td>");
      out.println("<td>" + user.getUserId() + "</td>");
      out.println("<td>" + user.getIsAdmin() + "</td>");
      out.println("<td>" + user.getAccessKey() + "</td>");
      out.println("<td>" + user.getAccessSecret() + "</td>");
      out.println("<td><a href='/webui/cnsuser?userId=" + user.getUserId() + "'>CNS</a></td>");
      out.println("<td><a href='/webui/cqsuser?userId=" + user.getUserId() + "'>CQS</a></td>");

      out.println(
          "<td><input type='submit' value='Delete' name='Delete' onclick=\"return confirm('Are you sure you want to delete user "
              + user.getUserName()
              + "?')\" /></td></form></tr>");
    }

    out.println("</table></body></html>");

    CMBControllerServlet.valueAccumulator.deleteAllCounters();
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    doGet(request, response);
  }
}
