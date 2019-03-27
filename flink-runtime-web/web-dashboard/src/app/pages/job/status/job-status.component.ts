/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Input, OnDestroy, OnInit } from '@angular/core';
import { JobDetailCorrectInterface } from 'interfaces';
import { Subject } from 'rxjs';
import { distinctUntilKeyChanged, takeUntil } from 'rxjs/operators';
import { JobService } from 'services';

@Component({
  selector: 'flink-job-status',
  templateUrl: './job-status.component.html',
  styleUrls: ['./job-status.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobStatusComponent implements OnInit, OnDestroy {
  @Input() isLoading = true;
  private destroy$ = new Subject();
  statusTips: string;
  jobDetail: JobDetailCorrectInterface;
  listOfNavigation = [
    {
      path: 'overview',
      title: 'Overview'
    },
    {
      path: 'exceptions',
      title: 'Exceptions'
    },
    {
      path: 'timeline',
      title: 'TimeLine'
    },
    {
      path: 'checkpoints',
      title: 'Checkpoints'
    },
    {
      path: 'configuration',
      title: 'Configuration'
    }
  ];

  stopJob() {
    this.jobService.stopJob(this.jobDetail.jid).subscribe(() => {
      this.statusTips = 'Stopping...';
      this.cdr.markForCheck();
    });
  }

  cancelJob() {
    this.jobService.cancelJob(this.jobDetail.jid).subscribe(() => {
      this.statusTips = 'Cancelling...';
      this.cdr.markForCheck();
    });
  }

  constructor(private jobService: JobService, private cdr: ChangeDetectorRef) {}

  ngOnInit() {
    const jobDetail$ = this.jobService.jobDetail$.pipe(takeUntil(this.destroy$));
    jobDetail$.subscribe(data => {
      this.jobDetail = data;
      this.cdr.markForCheck();
    });
    jobDetail$.pipe(distinctUntilKeyChanged('state')).subscribe(() => {
      this.statusTips = '';
    });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
