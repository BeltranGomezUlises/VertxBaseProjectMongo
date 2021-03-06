/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package service.commons;

import static database.commons.Action.*;
import database.commons.ErrorCodes;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import java.util.Date;
import models.PropertyError;
import static service.commons.Constants.*;
import utils.UtilsJWT;
import static utils.UtilsResponse.*;
import utils.UtilsRouter;
import utils.UtilsValidation;

/**
 * Base Verticle to work with LCRUD default operations
 *
 * @author Ulises Beltrán Gómez - beltrangomezulises@gmail.com
 */
public abstract class ServiceVerticle extends AbstractVerticle {

    /**
     * The router for this verticle service instance
     */
    protected final Router router = Router.router(vertx);

    /**
     * Need to specifie the address of the verticles in the event bus with the
     * access of the db that contains the table
     *
     * @return the name of the registered DBVerticle to work with
     */
    protected abstract String getDBAddress();

    /**
     * Need to especifie the endpoint domain for this verticles begining with
     * "/", ex: return "/example";
     *
     * @return the name to register the verticle in the main router
     */
    protected abstract String getEndpointAddress();

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        HttpServer server = vertx.createHttpServer();
        router.get("/").handler(this::findAll);
        router.get("/:id").handler(this::findById);
        router.get("/action/count").handler(this::count);
        router.post("/").handler(BodyHandler.create()); //needed to catch body of request
        router.post("/").handler(this::create);
        router.put("/").handler(BodyHandler.create()); //needed to catch body of request
        router.put("/").handler(this::update);
        router.delete("/:id").handler(this::deleteById);
        router.delete("/action/hide/:id").handler(this::hideById);
        UtilsRouter.getInstance(vertx).mountSubRouter(getEndpointAddress(), router);
        Integer portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT);
        if (portNumber == null) {
            startFuture.fail(new Exception("No port speficied in configuration"));
            System.out.println("Could not start a HTTP server" + this.getClass().getSimpleName() + ", no port speficied in configuration");
        }
        server.requestHandler(UtilsRouter.getInstance(vertx)::accept)
                .listen(portNumber, ar -> {
                    if (ar.succeeded()) {
                        System.out.println(this.getClass().getSimpleName() + " running");
                        startFuture.complete();
                    } else {
                        System.out.println("Could not start a HTTP server " + this.getClass().getSimpleName() + ", " + ar.cause());
                        startFuture.fail(ar.cause());
                    }
                });
    }

    /**
     * Sends a message to the verticle registered with DBAddress especified in
     * this instance the action of "findAll"
     *
     * @param context the routing context running in the request
     */
    protected void findAll(RoutingContext context) {
        String jwt = context.request().getHeader("Authorization");
        if (UtilsJWT.isTokenValid(jwt)) {
            JsonObject body = new JsonObject()
                    .put("select", context.request().getParam("select"))
                    .put("query", context.request().getParam("query"))
                    .put("from", context.request().getParam("from"))
                    .put("to", context.request().getParam("to"));
            DeliveryOptions options = new DeliveryOptions().addHeader(ACTION, FIND_ALL.name());
            vertx.eventBus().send(this.getDBAddress(), body, options, reply -> {
                if (reply.succeeded()) {
                    responseOk(context, reply.result().body(), "Found");
                } else {
                    responseError(context, UNEXPECTED_ERROR, reply.cause().getMessage());
                }
            });
        } else {
            responseInvalidToken(context);
        }
    }

    /**
     * Sends a message to the verticle registered with DBAddress especified in
     * this instance the action of "findById"
     *
     * @param context the routing context running in the request
     */
    protected void findById(RoutingContext context) {
        String jwt = context.request().getHeader("Authorization");
        if (UtilsJWT.isTokenValid(jwt)) {
            JsonObject message = new JsonObject().put("_id", context.request().getParam("id"));
            DeliveryOptions options = new DeliveryOptions().addHeader(ACTION, FIND_BY_ID.name());
            vertx.eventBus().send(this.getDBAddress(), message, options, reply -> {
                if (reply.succeeded()) {
                    responseOk(context, reply.result().body(), "Found");
                } else {
                    responseError(context, UNEXPECTED_ERROR, reply.cause().getMessage());
                }
            });
        } else {
            responseInvalidToken(context);
        }
    }

    /**
     * Sends a message to the verticle registered with DBAddress especified in
     * this instance the action of "update"
     *
     * @param context the routing context running in the request
     */
    protected void update(RoutingContext context) {
        String jwt = context.request().getHeader("Authorization");
        if (UtilsJWT.isTokenValid(jwt)) {
            if (this.isValidUpdateData(context)) {
                DeliveryOptions options = new DeliveryOptions().addHeader(ACTION, UPDATE.name());
                JsonObject reqBody = context.getBodyAsJson();
                //clean properties if exist any of this
                reqBody.remove("created_at");
                reqBody.remove("created_by");
                //set the user requesting to update
                reqBody.put("updated_at", new Date().getTime());
                reqBody.put("updated_by", UtilsJWT.getUserIdFrom(jwt));
                vertx.eventBus().send(this.getDBAddress(), reqBody, options, reply -> {
                    if (reply.succeeded()) {
                        MultiMap headers = reply.result().headers();
                        if (headers.contains(ErrorCodes.DB_ERROR.toString())) {
                            responseWarning(context, headers.get(ErrorCodes.DB_ERROR.name()));
                        } else {
                            responseOk(context, "Updated");
                        }
                    } else {
                        responseError(context, UNEXPECTED_ERROR, reply.cause().getMessage());
                    }
                });
            }
        } else {
            responseInvalidToken(context);
        }
    }

    /**
     * Sends a message to the verticle registered with DBAddress especified in
     * this instance the action of "create"
     *
     * @param context the routing context running in the request
     */
    protected void create(RoutingContext context) {
        String jwt = context.request().getHeader("Authorization");
        if (UtilsJWT.isTokenValid(jwt)) {
            if (this.isValidCreateData(context)) {
                DeliveryOptions options = new DeliveryOptions().addHeader(ACTION, CREATE.name());
                JsonObject reqBody = context.getBodyAsJson();
                reqBody.put("created_at", new Date().getTime());
                reqBody.put("created_by", UtilsJWT.getUserIdFrom(jwt));
                reqBody.put("active", true);
                reqBody.remove("updated_at");
                reqBody.remove("updated_by");
                vertx.eventBus().send(this.getDBAddress(), reqBody, options, reply -> {
                    if (reply.succeeded()) {
                        if (reply.result().headers().contains(ErrorCodes.DB_ERROR.toString())) {
                            responseWarning(context, INVALID_DATA, INVALID_DATA_MESSAGE, reply.result().body());
                        } else {
                            responseOk(context, reply.result().body(), "Created");
                        }
                    } else {
                        responseError(context, UNEXPECTED_ERROR, reply.cause().getMessage());
                    }
                });
            }
        } else {
            responseInvalidToken(context);
        }
    }

    /**
     * Sends a message to the verticle registered with DBAddress especified in
     * this instance the action of "deleteById"
     *
     * @param context the routing context running in the request
     */
    protected void deleteById(RoutingContext context) {
        String jwt = context.request().getHeader("Authorization");
        if (UtilsJWT.isTokenValid(jwt)) {
            JsonObject reqBody = new JsonObject().put("_id", context.request().getParam("id"));
            DeliveryOptions options = new DeliveryOptions().addHeader(ACTION, DELETE_BY_ID.name());
            vertx.eventBus().send(this.getDBAddress(), reqBody, options,
                    reply -> {
                        if (reply.succeeded()) {
                            MultiMap headers = reply.result().headers();
                            if (headers.contains(ErrorCodes.DB_ERROR.toString())) {
                                responseWarning(context, headers.get(ErrorCodes.DB_ERROR.name()));
                            } else {
                                responseOk(context, "Deleted");
                            }
                        } else {
                            responseError(context, UNEXPECTED_ERROR, reply.cause().getMessage());
                        }
                    }
            );
        } else {
            responseWarning(context, "Out of session", "Sessión json web token is invalid");
        }
    }

    /**
     * Sends a message to the verticle registered with DBAddress especified in
     * this instance the action of "hideById"
     *
     * @param context the routing context running in the request
     */
    protected void hideById(RoutingContext context) {
        String jwt = context.request().getHeader("Authorization");
        if (UtilsJWT.isTokenValid(jwt)) {
            JsonObject reqBody = new JsonObject().put("_id", context.request().getParam("id"));
            DeliveryOptions options = new DeliveryOptions().addHeader(ACTION, HIDE_BY_ID.name());
            vertx.eventBus().send(this.getDBAddress(), reqBody, options,
                    reply -> {
                        if (reply.succeeded()) {
                            MultiMap headers = reply.result().headers();
                            if (headers.contains(ErrorCodes.DB_ERROR.toString())) {
                                responseWarning(context, headers.get(ErrorCodes.DB_ERROR.name()));
                            } else {
                                responseOk(context, "Hided");
                            }
                        } else {
                            responseError(context, UNEXPECTED_ERROR, reply.cause().getMessage());
                        }
                    }
            );
        } else {
            responseWarning(context, "Out of session", "Sessión json web token is invalid");
        }
    }

    /**
     * Sends a message to the verticle registered with DBAddress especified in
     * this instance the action of "deleteById"
     *
     * @param context the routing context running in the request
     */
    protected void count(RoutingContext context) {
        String jwt = context.request().getHeader("Authorization");
        if (UtilsJWT.isTokenValid(jwt)) {
            DeliveryOptions options = new DeliveryOptions().addHeader(ACTION, COUNT.name());
            vertx.eventBus().send(this.getDBAddress(), null, options,
                    reply -> {
                        if (reply.succeeded()) {
                            MultiMap headers = reply.result().headers();
                            if (headers.contains(ErrorCodes.DB_ERROR.toString())) {
                                responseWarning(context, headers.get(ErrorCodes.DB_ERROR.name()));
                            } else {
                                responseOk(context, reply.result().body(), "Counted");
                            }
                        } else {
                            responseError(context, UNEXPECTED_ERROR, reply.cause().getMessage());
                        }
                    }
            );
        } else {
            responseWarning(context, "Out of session", "Session json web token is invalid");
        }
    }

    /**
     * Verifies is the data of the request is valid to create a record of this
     * entity
     *
     * @param context context of the request
     * @return true if the data is valid, false othrewise
     */
    protected boolean isValidCreateData(RoutingContext context) {
        if (context.getBodyAsJson().getInteger("_id") != null) {
            responseWarning(context, INVALID_DATA, INVALID_DATA_MESSAGE, new PropertyError("id", UtilsValidation.INVALID_PARAMETER));
            return false;
        }
        return true;
    }

    /**
     * Verifies is the data of the request is valid to update a record of this
     * entity
     *
     * @param context context of the request
     * @return true if the data is valid, false othrewise
     */
    protected boolean isValidUpdateData(RoutingContext context) {
        if (context.getBodyAsJson().getString("_id") == null) {
            responseWarning(context, INVALID_DATA, INVALID_DATA_MESSAGE, new PropertyError("id", UtilsValidation.MISSING_REQUIRED_VALUE));
            return false;
        }
        return true;
    }

}
