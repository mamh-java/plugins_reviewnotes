// Copyright (C) 2013 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.googlesource.gerrit.plugins.reviewnotes;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.flogger.FluentLogger;
import com.google.gerrit.common.Nullable;
import com.google.gerrit.entities.Change;
import com.google.gerrit.entities.LabelType;
import com.google.gerrit.entities.LabelTypes;
import com.google.gerrit.entities.PatchSet;
import com.google.gerrit.entities.PatchSetApproval;
import com.google.gerrit.entities.Project;
import com.google.gerrit.extensions.registration.DynamicItem;
import com.google.gerrit.git.LockFailureException;
import com.google.gerrit.git.ObjectIds;
import com.google.gerrit.server.GerritPersonIdent;
import com.google.gerrit.server.PatchSetUtil;
import com.google.gerrit.server.account.AccountCache;
import com.google.gerrit.server.account.AccountState;
import com.google.gerrit.server.approval.ApprovalsUtil;
import com.google.gerrit.server.config.AnonymousCowardName;
import com.google.gerrit.server.config.UrlFormatter;
import com.google.gerrit.server.git.NotesBranchUtil;
import com.google.gerrit.server.notedb.ChangeNotes;
import com.google.gerrit.server.project.NoSuchChangeException;
import com.google.gerrit.server.project.ProjectCache;
import com.google.gerrit.server.project.ProjectState;
import com.google.gerrit.server.query.change.ChangeData;
import com.google.gerrit.server.query.change.InternalChangeQuery;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.assistedinject.Assisted;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.NullProgressMonitor;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.ProgressMonitor;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.notes.NoteMap;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;

class CreateReviewNotes {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  interface Factory {
    CreateReviewNotes create(Project.NameKey project, Repository git);
  }

  private static final String REFS_NOTES_REVIEW = "refs/notes/review";

  private final PersonIdent gerritServerIdent;
  private final AccountCache accountCache;
  private final String anonymousCowardName;
  private final LabelTypes labelTypes;
  private final ApprovalsUtil approvalsUtil;
  private final ChangeData.Factory changeDataFactory;
  private final ChangeNotes.Factory notesFactory;
  private final NotesBranchUtil.Factory notesBranchUtilFactory;
  private final Provider<InternalChangeQuery> queryProvider;
  private final DynamicItem<UrlFormatter> urlFormatter;
  private final PatchSetUtil psUtil;
  private final Project.NameKey project;
  private final Repository git;

  private ObjectInserter inserter;
  private NoteMap reviewNotes;
  private StringBuilder message;

  @Inject
  CreateReviewNotes(
      @GerritPersonIdent PersonIdent gerritIdent,
      AccountCache accountCache,
      @AnonymousCowardName String anonymousCowardName,
      ProjectCache projectCache,
      ApprovalsUtil approvalsUtil,
      ChangeData.Factory changeDataFactory,
      ChangeNotes.Factory notesFactory,
      NotesBranchUtil.Factory notesBranchUtilFactory,
      Provider<InternalChangeQuery> queryProvider,
      DynamicItem<UrlFormatter> urlFormatter,
      PatchSetUtil psUtil,
      @Assisted Project.NameKey project,
      @Assisted Repository git) {
    this.gerritServerIdent = gerritIdent;
    this.accountCache = accountCache;
    this.anonymousCowardName = anonymousCowardName;
    Optional<ProjectState> projectState = projectCache.get(project);
    if (!projectState.isPresent()) {
      logger.atSevere().log(
          "Could not obtain available labels for project %s."
              + " Expect missing labels in its review notes.",
          project.get());
      this.labelTypes = new LabelTypes(Collections.<LabelType>emptyList());
    } else {
      this.labelTypes = projectState.get().getLabelTypes();
    }
    this.approvalsUtil = approvalsUtil;
    this.changeDataFactory = changeDataFactory;
    this.notesFactory = notesFactory;
    this.notesBranchUtilFactory = notesBranchUtilFactory;
    this.queryProvider = queryProvider;
    this.urlFormatter = urlFormatter;
    this.psUtil = psUtil;
    this.project = project;
    this.git = git;
  }

  void createNotes(
      String branch, ObjectId oldObjectId, ObjectId newObjectId, ProgressMonitor monitor)
      throws IOException {
    if (ObjectId.zeroId().equals(newObjectId)) {
      return;
    }

    try (RevWalk rw = new RevWalk(git)) {
      try {
        RevCommit n = rw.parseCommit(newObjectId);
        rw.markStart(n);
        if (n.getParentCount() == 1 && n.getParent(0).equals(oldObjectId)) {
          rw.markUninteresting(rw.parseCommit(oldObjectId));
        } else {
          markUninteresting(git, branch, rw, oldObjectId);
        }
      } catch (Exception e) {
        logger.atSevere().withCause(e).log("%s", e.getMessage());
        return;
      }

      if (monitor == null) {
        monitor = NullProgressMonitor.INSTANCE;
      }

      for (RevCommit c : rw) {
        PatchSet ps = loadPatchSet(c, branch);
        if (ps != null) {
          ChangeNotes notes = notesFactory.create(project, ps.id().changeId());
          ObjectId content = createNoteContent(notes, ps);
          if (content != null) {
            monitor.update(1);
            getNotes().set(c, content);
            getMessage().append("* ").append(c.getShortMessage()).append("\n");
          }
        } else {
          logger.atFine().log(
              "no note for this commit since it is a direct push %s", c.getName().substring(0, 7));
        }
      }
    }
  }

  void createNotes(List<ChangeNotes> notes, ProgressMonitor monitor) throws IOException {
    try (RevWalk rw = new RevWalk(git)) {
      if (monitor == null) {
        monitor = NullProgressMonitor.INSTANCE;
      }

      for (ChangeNotes cn : notes) {
        monitor.update(1);
        PatchSet ps = psUtil.current(cn);
        RevCommit commit = rw.parseCommit(ps.commitId());
        getNotes().set(commit, createNoteContent(cn, ps));
        getMessage().append("* ").append(commit.getShortMessage()).append("\n");
      }
    }
  }

  void commitNotes() throws LockFailureException, IOException {
    try {
      if (reviewNotes == null) {
        return;
      }

      message.insert(0, "Update notes for submitted changes\n\n");
      notesBranchUtilFactory
          .create(project, git, inserter)
          .commitAllNotes(reviewNotes, REFS_NOTES_REVIEW, gerritServerIdent, message.toString());
    } finally {
      if (inserter != null) {
        inserter.close();
      }
    }
  }

  private void markUninteresting(Repository git, String branch, RevWalk rw, ObjectId oldObjectId)
      throws IOException {
    for (Ref r : git.getRefDatabase().getRefs()) {
      try {
        if (r.getName().equals(branch)) {
          if (!ObjectId.zeroId().equals(oldObjectId)) {
            // For the updated branch the oldObjectId is the tip of uninteresting
            // commit history
            rw.markUninteresting(rw.parseCommit(oldObjectId));
          }
        } else if (r.getName().startsWith(Constants.R_HEADS)
            || r.getName().startsWith(Constants.R_TAGS)) {
          rw.markUninteresting(rw.parseCommit(r.getObjectId()));
        }
      } catch (IncorrectObjectTypeException e) {
        // skip if not parseable as a commit
      } catch (MissingObjectException e) {
        // skip if not parseable as a commit
      } catch (IOException e) {
        // skip if not parseable as a commit
      }
    }
  }

  @Nullable
  private ObjectId createNoteContent(ChangeNotes notes, PatchSet ps) throws IOException {
    HeaderFormatter fmt = new HeaderFormatter(gerritServerIdent.getZoneId(), anonymousCowardName);
    if (ps != null) {
      try {
        createCodeReviewNote(notes, ps, fmt);
        return getInserter().insert(Constants.OBJ_BLOB, fmt.toString().getBytes(UTF_8));
      } catch (NoSuchChangeException e) {
        throw new IOException(e);
      }
    }
    return null;
  }

  @Nullable
  private PatchSet loadPatchSet(RevCommit c, String destBranch) {
    String hash = c.name();
    for (ChangeData cd : queryProvider.get().byBranchCommit(project.get(), destBranch, hash)) {
      for (PatchSet ps : cd.patchSets()) {
        if (ObjectIds.matchesAbbreviation(ps.commitId(), hash)) {
          return ps;
        }
      }
    }
    return null; // TODO: createNoCodeReviewNote(branch, c, fmt);
  }

  private void createCodeReviewNote(ChangeNotes notes, PatchSet ps, HeaderFormatter fmt)
      throws NoSuchChangeException {
    // This races with the label normalization/writeback done by MergeOp. It may
    // repeat some work, but results should be identical except in the case of
    // an additional race with a permissions change.
    // TODO(dborowitz): These will eventually be stamped in the ChangeNotes at
    // commit time so we will be able to skip this normalization step.
    Change change = notes.getChange();
    PatchSetApproval submit = null;
    for (PatchSetApproval a : approvalsUtil.byPatchSet(notes, ps.id())) {
      if (a.value() == 0) {
        // Ignore 0 values.
      } else if (a.isLegacySubmit()) {
        submit = a;
      } else {
        Optional<LabelType> type = labelTypes.byLabel(a.labelId());
        if (type.isPresent()) {
          fmt.appendApproval(
              type.get(),
              a.value(),
              a.accountId(),
              accountCache.get(a.accountId()).map(AccountState::account));
        }
      }
    }
    if (submit != null) {
      fmt.appendSubmittedBy(
          submit.accountId(), accountCache.get(submit.accountId()).map(AccountState::account));
      fmt.appendSubmittedAt(submit.granted());
    }

    UrlFormatter uf = urlFormatter.get();
    if (uf != null && uf.getWebUrl().isPresent()) {
      fmt.appendReviewedOn(uf, notes.getChange().getProject(), ps.id().changeId());
    }

    ChangeData cd = changeDataFactory.create(notes);
    fmt.appendCommentCount(cd.totalCommentCount(), cd.unresolvedCommentCount());

    fmt.appendProject(project.get());
    fmt.appendBranch(change.getDest().branch());
  }

  private ObjectInserter getInserter() {
    if (inserter == null) {
      inserter = git.newObjectInserter();
    }
    return inserter;
  }

  private NoteMap getNotes() {
    if (reviewNotes == null) {
      reviewNotes = NoteMap.newEmptyMap();
    }
    return reviewNotes;
  }

  private StringBuilder getMessage() {
    if (message == null) {
      message = new StringBuilder();
    }
    return message;
  }
}
