import {Component, Input, ElementRef, Inject, Output, EventEmitter, Directive, OnInit}      from '@angular/core';

import 'rxjs/Rx';

import {CandidatesService, NerAnnotation, TwitterAnnotation, Score, ScoreBundle} from "../services/api";

@Component({
    selector: '[smt-candidate]',
    templateUrl: '../../templates/candidate.html',
    providers: [ ]
})
export class CandidateRow implements OnInit {
    @Input() candidate : Object;
    @Input() scores : Score;

    scoreClick(score : Score) {
        console.log(score);
    }

    ngOnInit() { }
}

@Component({
    selector: 'smt-alignment-result',
    templateUrl: '../../templates/alignment-result.html',
    providers: [ ]
})
export class AlignmentResult {
    @Input('annotation') private annotation : NerAnnotation;
    @Output() selected = new EventEmitter<NerAnnotation>();

    getClassName() {
        switch(this.annotation.nerClass) {
            case "PERSON":
                return "label label-success";
            case "ORGANIZATION":
                return "label label-primary";
            default:
                return "";
        }
    }

    selectAnnotation() {
        this.selected.emit(this.annotation);
    }
}

@Component({
    selector: 'smt-annotate',
    templateUrl: '../../templates/annotate.html',
    providers: [ CandidatesService ],
    viewProviders: [ AlignmentResult, CandidateRow ]
})
export class Annotate {
    private errorMessage : string;
    private annotations : NerAnnotation[];
    private selectedAnnotation : NerAnnotation;
    private twitterAnnotation : TwitterAnnotation;
    private selectedScoreBundle : ScoreBundle;
    private annotationRepository : Candidate[];
    private scoreTypeDict = {
        "cos_sim+bow": "BOW",
        "cos_sim+bow_claudio": "BOW Claudio",
        "cos_sim+lsa": "LSA",
        "alignments": "Alignments"
    };

    getTypeName(type : string) {
        if (typeof this.scoreTypeDict[type] == 'undefined') {
            return type;
        }
        return this.scoreTypeDict[type];
    }

    annotate(searchString : string) {
        let self = this;
        this.selectedAnnotation = null;
        this.annotations = [];
        this.candidatesService.getNerAnnotation(searchString).subscribe(
            function (annotations) {
                self.annotations = annotations;
            },
            function (error) {
                self.annotations = null;
                self.errorMessage = <any>error;
            }
        );
    }

    selectAnnotation(text : string, annotation : NerAnnotation) {
        let self = this;
        this.selectedAnnotation = annotation;
        self.selectedScoreBundle = null;
        this.candidatesService.getTwitterAnnotation(text, this.selectedAnnotation.token, this.selectedAnnotation.nerClass).subscribe(
            function (annotation) {
                self.twitterAnnotation = annotation;
                if (annotation.results.length > 0) {
                    self.selectedScoreBundle = annotation.results[0];
                }
                let candidates = {};
                for (let scoreBundle of annotation.results) {
                    for (let score of scoreBundle.scores) {
                        if (typeof candidates[score.username] == 'undefined') {
                            candidates[score.username] = new Candidate(score.username);
                        }
                        let candidate = candidates[score.username];
                        candidate.scores.push({
                            type: scoreBundle.type,
                            score: score.score,
                            debug: score.debug
                        });
                    }
                }
                self.annotationRepository = [];
                for (let candidate of Object.values(candidates)) {
                    self.annotationRepository.push(candidate);
                }
            },
            function (error) {
                self.twitterAnnotation = null;
                self.errorMessage = <any>error;
            }
        )
    }

    constructor(private candidatesService : CandidatesService) {
        this.annotations = [];
        this.selectedAnnotation = null;
        this.twitterAnnotation = null;
    };
}

export class Candidate {
    public username : string;
    public scores : Object[];

    constructor(name : string) {
        this.username = name;
        this.scores = [];
    };
}