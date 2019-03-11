/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package database.commons;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;
import static service.commons.Constants.ACTION;

/**
 *
 * @author ulises
 */
public abstract class DBVerticle extends AbstractVerticle {

    /**
     * the client contains the channel of comunication with the database
     */
    protected MongoClient dbClient;

    /**
     * method that runs when the verticles is deployed
     *
     * @param startFuture future to start with this deployment
     * @throws Exception
     */
    @Override
    public void start(Future<Void> startFuture) throws Exception {
        dbClient = MongoClient.createShared(vertx, config());
        this.vertx.eventBus().consumer(this.getClass().getSimpleName(), this::onMessage);
        startFuture.complete();
    }

    /**
     * This method takes the action of the message and execute the method that corresponds
     *
     * @param message the message from the event bus
     */
    protected void onMessage(Message<JsonObject> message) {
        if (isValidAction(message)) {
            try {
                Action action = Action.valueOf(message.headers().get(ACTION));
                switch (action) {
                    case CREATE:
                        this.create(message);
                        break;
                    case DELETE_BY_ID:
                        this.deleteById(message);
                        break;
                    case HIDE_BY_ID:
                        this.hideById(message);
                        break;
                    case FIND_BY_ID:
                        this.findById(message);
                        break;
                    case FIND_ALL:
                        this.findAll(message);
                        break;
                    case UPDATE:
                        this.update(message);
                        break;
                    case COUNT:
                        this.count(message);
                        break;
                }
            } catch (IllegalArgumentException e) {
            }
        }
    }

    /**
     * Validates if the action in the headers is valid
     *
     * @param message the message from the event bus
     * @return true if containg an action, false otherwise
     */
    protected boolean isValidAction(Message<JsonObject> message) {
        if (!message.headers().contains("action")) {
            message.fail(ErrorCodes.NO_ACTION_SPECIFIED.ordinal(), "No action header specified");
            return false;
        }
        return true;
    }

    /**
     * Execute the query "select * from"
     *
     * @param message message from the event bus
     */
    protected void findAll(Message<JsonObject> message) {
        JsonObject body = message.body();
        JsonObject queryObject = new JsonObject();
        FindOptions findOptions = new FindOptions();

        //set projection
        String select = body.getString("select");
        if (select != null) {
            JsonObject objectFields = new JsonObject();
            for (String field : select.split(",")) {
                objectFields.put(field, 1);
            }
            findOptions.setFields(objectFields);
        }

        //set pagination
        String from = body.getString("from");
        if (from != null) {
            String to = body.getString("to");
            if (to != null) {
                try {
                    int fromValue = Integer.parseInt(from);
                    int toValue = Integer.parseInt(to);
                    findOptions.setSkip(fromValue).setLimit(toValue);
                } catch (Exception e) {
                }
            }
        }

        //set query
        String query = body.getString("query");
        if (query != null) {
            addQueriestoObject(queryObject, query);
        }

        this.dbClient.findWithOptions(this.getEntityName(), queryObject, findOptions, reply -> {
            if (reply.succeeded()) {
                message.reply(new JsonArray(reply.result()));
            } else {
                message.fail(ErrorCodes.DB_ERROR.ordinal(), reply.cause().getMessage());
            }
        });
    }

    /**
     * Execute the query "select * from table where id = ?"
     *
     * @param message message from the event bus
     */
    protected void findById(Message<JsonObject> message) {
        this.dbClient.findOne(this.getEntityName(), message.body(), new JsonObject(), reply -> {
            if (reply.succeeded()) {
                message.reply(reply.result());
            } else {
                message.fail(ErrorCodes.DB_ERROR.ordinal(), reply.cause().getMessage());
            }
        });
    }

    /**
     * Execute the query "delete from table where id = ?"
     *
     * @param message message from the event bus
     */
    protected void deleteById(Message<JsonObject> message) {
        this.dbClient.removeDocument(this.getEntityName(), message.body(), reply -> {
            if (reply.succeeded()) {
                long removedCount = reply.result().getRemovedCount();
                if (removedCount == 0) {
                    message.reply(new JsonObject(), new DeliveryOptions().addHeader(ErrorCodes.DB_ERROR.name(), "Element not found"));
                } else {
                    message.reply(null);
                }
            } else {
                message.fail(ErrorCodes.DB_ERROR.ordinal(), reply.cause().getMessage());
            }
        });
    }

    /**
     * Execute the query "delete from table where id = ?"
     *
     * @param message message from the event bus
     */
    protected void hideById(Message<JsonObject> message) {
        JsonObject updateObject = new JsonObject()
                .put("$set", new JsonObject().put("active", false));
        this.dbClient.updateCollection(this.getEntityName(),
                message.body(), updateObject, reply -> {
            if (reply.succeeded()) {
                long removedCount = reply.result().getDocModified();
                if (removedCount == 0) {
                    message.reply(new JsonObject(), new DeliveryOptions().addHeader(ErrorCodes.DB_ERROR.name(), "Element not found"));
                } else {
                    message.reply(null);
                }
            } else {
                message.fail(ErrorCodes.DB_ERROR.ordinal(), reply.cause().getMessage());
            }
        });
    }

    /**
     * Execute the query "create" generated by the properties of the object in the message
     *
     * @param message message from the event bus
     */
    protected void create(Message<JsonObject> message) {
        dbClient.insert(this.getEntityName(), message.body(), reply -> {
            if (reply.succeeded()) {
                String id = reply.result();
                message.reply(new JsonObject().put("id", id));
            } else {
                message.fail(ErrorCodes.DB_ERROR.ordinal(), reply.cause().getMessage());
            }
        });
    }

    /**
     * Execute the query "update" generated by the properties of the object in the message
     *
     * @param message message from the event bus
     */
    protected void update(Message<JsonObject> message) {
        JsonObject query = new JsonObject()
                .put("_id", message.body().getString("_id"));
        JsonObject body = message.body();
        body.remove("_id");
        JsonObject update = new JsonObject()
                .put("$set", body);
        this.dbClient.updateCollection(this.getEntityName(), query, update,
                reply -> {
                    if (reply.succeeded()) {
                        long removedCount = reply.result().getDocModified();
                        if (removedCount == 0) {
                            message.reply(new JsonObject(), new DeliveryOptions().addHeader(ErrorCodes.DB_ERROR.name(), "Element not found"));
                        } else {
                            message.reply(null);
                        }
                    } else {
                        message.fail(ErrorCodes.DB_ERROR.ordinal(), reply.cause().getMessage());
                    }
                });
    }

    /**
     * Execute the count query of all elements in the table of this verticle
     *
     * @param message message from the event bus
     */
    protected void count(Message<JsonObject> message) {
        this.dbClient.count(this.getEntityName(), new JsonObject(), reply -> {
            if (reply.succeeded()) {
                message.reply(reply.result());
            } else {
                message.fail(ErrorCodes.DB_ERROR.ordinal(), reply.cause().getMessage());
            }
        });
    }

    /**
     * adds the query param to a json object with the structure of the comparators
     *
     * @param ob object to set the queries
     * @param query query param with the properties and values coma separated
     */
    private void addQueriestoObject(JsonObject ob, String query) {
        String[] queries = query.split(",");
        for (String q : queries) {
            ModelSelector modelSelector = whereSelection(q);
            if (modelSelector != null) {
                String[] values = q.split(modelSelector.getSelector());
                if (values[1].matches("-?\\d+(\\.\\d+)?")) { //if is number
                    ob.put(values[0], new JsonObject().put(modelSelector.getComparator(), Double.parseDouble(values[1])));
                } else {
                    ob.put(values[0], new JsonObject().put(modelSelector.getComparator(), values[1]));
                }
            }
        }
    }

    /**
     * check if there is a where condition in a selection
     *
     * @param selection the field selection
     * @return the operator to use in the where clause like '=' or '!=' or null is does not have one
     */
    private ModelSelector whereSelection(String selection) {
        ModelSelector model = null;
        if (selection.contains(">=")) {
            model = new ModelSelector(">=", "$gte");
        }
        if (selection.contains("<=")) {
            model = new ModelSelector("<=", "$lte");
        }
        if (selection.contains("!=")) {
            model = new ModelSelector("!=", "$ne");
        }
        if (selection.contains("=")) {
            model = new ModelSelector("=", "$eq");
        }
        if (selection.contains(">")) {
            model = new ModelSelector(">", "$gt");
        }
        if (selection.contains("<")) {
            model = new ModelSelector("<", "$lt");
        }
        return model;
    }

    /**
     * Need to especifie the name of the entity
     *
     * @return the name of the table to manage in this verticle
     */
    public abstract String getEntityName();

    private static class ModelSelector {

        private String selector;
        private String comparator;

        public ModelSelector(String selector, String comparator) {
            this.selector = selector;
            this.comparator = comparator;
        }

        public String getSelector() {
            return selector;
        }

        public void setSelector(String selector) {
            this.selector = selector;
        }

        public String getComparator() {
            return comparator;
        }

        public void setComparator(String comparator) {
            this.comparator = comparator;
        }

    }
}
