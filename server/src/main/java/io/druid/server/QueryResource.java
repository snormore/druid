/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.query.DataSourceUtil;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryWatcher;
import io.druid.server.log.RequestLogger;
import org.joda.time.DateTime;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.UUID;

/**
 */
@Path("/druid/v2/")
public class QueryResource
{
  private static final EmittingLogger log = new EmittingLogger(QueryResource.class);
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final Joiner COMMA_JOIN = Joiner.on(",");

  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final QuerySegmentWalker texasRanger;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final QueryManager queryManager;

  @Inject
  public QueryResource(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QuerySegmentWalker texasRanger,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      QueryManager queryManager
  )
  {
    this.jsonMapper = jsonMapper.copy();
    this.jsonMapper.getFactory().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

    this.smileMapper = smileMapper.copy();
    this.smileMapper.getFactory().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

    this.texasRanger = texasRanger;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.queryManager = queryManager;
  }

  @DELETE
  @Path("{id}")
  @Produces("application/json")
  public Response getServer(@PathParam("id") String queryId)
  {
    queryManager.cancelQuery(queryId);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  @POST
  @Produces("application/json")
  public void doPost(
      @Context HttpServletRequest req,
      @Context final HttpServletResponse resp
  ) throws ServletException, IOException
  {
    final long start = System.currentTimeMillis();
    Query query = null;
    byte[] requestQuery = null;
    String queryId;

    final boolean isSmile = "application/smile".equals(req.getContentType());

    ObjectMapper objectMapper = isSmile ? smileMapper : jsonMapper;
    ObjectWriter jsonWriter = req.getParameter("pretty") == null
                              ? objectMapper.writer()
                              : objectMapper.writerWithDefaultPrettyPrinter();
    OutputStream out = null;

    try {
      requestQuery = ByteStreams.toByteArray(req.getInputStream());
      query = objectMapper.readValue(requestQuery, Query.class);
      queryId = query.getId();
      if (queryId == null) {
        queryId = UUID.randomUUID().toString();
        query = query.withId(queryId);
      }

      if (log.isDebugEnabled()) {
        log.debug("Got query [%s]", query);
      }

      Sequence results = query.run(texasRanger);

      if (results == null) {
        results = Sequences.empty();
      }

      out = resp.getOutputStream();

      JsonGenerator jgen = objectMapper.getFactory().createGenerator(out);

      final String queryIdHeader = query.getId();
      results = onAccumulate(results, new Runnable()
                   {
                     @Override
                     public void run()
                     {
                       resp.setStatus(200);
                       resp.setContentType("application/x-javascript");
                       resp.setHeader("X-Druid-Query-Id", queryIdHeader);
                     }
                   });

      jsonWriter.writeValue(out, results);
      out.write("\n".getBytes(Charsets.UTF_8));

      long requestTime = System.currentTimeMillis() - start;

      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser2(DataSourceUtil.getMetricName(query.getDataSource()))
              .setUser4(query.getType())
              .setUser5(COMMA_JOIN.join(query.getIntervals()))
              .setUser6(String.valueOf(query.hasFilters()))
              .setUser7(req.getRemoteAddr())
              .setUser8(queryId)
              .setUser9(query.getDuration().toPeriod().toStandardMinutes().toString())
              .build("request/time", requestTime)
      );

      requestLogger.log(
          new RequestLogLine(
              new DateTime(),
              req.getRemoteAddr(),
              query,
              new QueryStats(
                  ImmutableMap.<String, Object>of(
                      "request/time", requestTime,
                      "success", true
                  )
              )
          )
      );
    }
    catch (Exception e) {
      final String queryString =
          query == null
          ? (isSmile ? "smile_unknown" : new String(requestQuery, Charsets.UTF_8))
          : query.toString();

      log.warn(e, "Exception occurred on request [%s]", queryString);

      if (!resp.isCommitted()) {
        resp.setStatus(500);
        resp.resetBuffer();

        if (out == null) {
          out = resp.getOutputStream();
        }

        out.write((e.getMessage() == null) ? "Exception null".getBytes(UTF8) : e.getMessage().getBytes(UTF8));
        out.write("\n".getBytes(UTF8));
      }

      resp.flushBuffer();

      try {
        requestLogger.log(
            new RequestLogLine(
                new DateTime(),
                req.getRemoteAddr(),
                query,
                new QueryStats(ImmutableMap.<String, Object>of("success", false, "exception", e.toString()))
            )
        );
      }
      catch (Exception e2) {
        log.error(e2, "Unable to log query [%s]!", queryString);
      }

      log.makeAlert(e, "Exception handling request")
         .addData("exception", e.toString())
         .addData("query", queryString)
         .addData("peer", req.getRemoteAddr())
         .emit();
    }
    finally {
      resp.flushBuffer();
      Closeables.closeQuietly(out);
    }
  }

  private static <T> Sequence onAccumulate(final Sequence<T> seq, final Runnable effect)
  {
    return new Sequence<T>()
    {
      @Override
      public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
      {
        effect.run();
        return seq.accumulate(initValue, accumulator);
      }

      @Override
      public <OutType> Yielder<OutType> toYielder(
          OutType initValue, YieldingAccumulator<OutType, T> accumulator
      )
      {
        return seq.toYielder(initValue, accumulator);
      }
    };
  }
}
