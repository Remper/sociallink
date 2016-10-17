import { Component }         from '@angular/core';

import 'rxjs/Rx';

import { CandidatesService } from '../services/api';

const CANDIDATES = [{"id":110104666,"name":"Yaroslav Nechaev","screenName":"BSearcher","location":"Trento, Trentino-Alto Adige","description":"PhD Student in Fondazione Bruno Kessler","descriptionURLEntities":[],"urlEntity":{"url":"http://t.co/FJ6GgMxgVI","expandedURL":"http://remper.ru","displayURL":"remper.ru","start":0,"end":22},"isContributorsEnabled":false,"profileImageUrl":"http://pbs.twimg.com/profile_images/538734792838094848/hR_mBrsz_normal.jpeg","profileImageUrlHttps":"https://pbs.twimg.com/profile_images/538734792838094848/hR_mBrsz_normal.jpeg","isDefaultProfileImage":false,"url":"http://t.co/FJ6GgMxgVI","isProtected":false,"followersCount":75,"status":{"createdAt":"Sep 6, 2016 2:22:08 PM","id":773134185439584256,"text":"@Unknown_Brother @xmbshwll С…РѕСЂРѕС€Рѕ РµС‰С‘ РјР°РјРєРµ РЅРµ РїРѕР·РІРѕРЅРёР»Рё","source":"\u003ca href\u003d\"http://twitter.com\" rel\u003d\"nofollow\"\u003eTwitter Web Client\u003c/a\u003e","isTruncated":false,"inReplyToStatusId":772835576370388993,"inReplyToUserId":486025124,"isFavorited":false,"isRetweeted":false,"favoriteCount":2,"inReplyToScreenName":"Unknown_Brother","place":{"name":"Trento","countryCode":"IT","id":"555588be28d4e2a1","country":"Italy","placeType":"city","url":"https://api.twitter.com/1.1/geo/id/555588be28d4e2a1.json","fullName":"Trento, Trentino-South Tyrol","boundingBoxType":"Polygon","boundingBoxCoordinates":[[{"latitude":45.975899,"longitude":11.0214658},{"latitude":45.975899,"longitude":11.1941383},{"latitude":46.1521934,"longitude":11.1941383},{"latitude":46.1521934,"longitude":11.0214658}]],"containedWithIn":[]},"retweetCount":0,"isPossiblySensitive":false,"lang":"ru","contributorsIDs":[],"userMentionEntities":[{"name":"Sergio says:","screenName":"Unknown_Brother","id":486025124,"start":0,"end":16},{"name":"Р›Р•Р’РђР¦РљРђРЇ РњР РђР—Р¬","screenName":"xmbshwll","id":117431089,"start":17,"end":26}],"urlEntities":[],"hashtagEntities":[],"mediaEntities":[],"extendedMediaEntities":[],"symbolEntities":[],"currentUserRetweetId":-1},"profileBackgroundColor":"0099B9","profileTextColor":"3C3940","profileLinkColor":"0099B9","profileSidebarFillColor":"95E8EC","profileSidebarBorderColor":"5ED4DC","profileUseBackgroundImage":true,"isDefaultProfile":false,"showAllInlineMedia":false,"friendsCount":71,"createdAt":"Jan 31, 2010 11:31:01 AM","favouritesCount":12,"utcOffset":10800,"timeZone":"Volgograd","profileBackgroundImageUrl":"http://abs.twimg.com/images/themes/theme4/bg.gif","profileBackgroundImageUrlHttps":"https://abs.twimg.com/images/themes/theme4/bg.gif","profileBackgroundTiled":false,"lang":"en","statusesCount":2210,"isGeoEnabled":true,"isVerified":false,"translator":false,"listedCount":13,"isFollowRequestSent":false},{"id":404746000,"name":"Yaroslav","screenName":"YaroslavNechaev","location":"Moscow","description":"-","descriptionURLEntities":[],"isContributorsEnabled":false,"profileImageUrl":"http://pbs.twimg.com/profile_images/1621902796/x_1a6773b4_normal.jpg","profileImageUrlHttps":"https://pbs.twimg.com/profile_images/1621902796/x_1a6773b4_normal.jpg","isDefaultProfileImage":false,"isProtected":false,"followersCount":4,"status":{"createdAt":"Jan 13, 2012 8:12:15 AM","id":157721603621388289,"text":"RT @RussianStyle_: Р Р°СЃСЃС‚СЂРµР» Р»СЋРґРµР№ РІ Р СѓР·Рµ РЅР° РіР»Р°Р·Р°С… Сѓ РїРѕР»РёС†РёРё РёР»Рё \"РЎС‚Р°РЅРёС†Р° РљСѓС‰РµРІСЃРєР°СЏ-2\" http://t.co/yltKP7IN","source":"\u003ca href\u003d\"http://golauncher.goforandroid.com\" rel\u003d\"nofollow\"\u003eGO Launcher EX\u003c/a\u003e","isTruncated":false,"inReplyToStatusId":-1,"inReplyToUserId":-1,"isFavorited":false,"isRetweeted":false,"favoriteCount":0,"retweetCount":1,"isPossiblySensitive":false,"lang":"ru","contributorsIDs":[],"retweetedStatus":{"createdAt":"Jan 13, 2012 12:00:42 AM","id":157597901235818496,"text":"Р Р°СЃСЃС‚СЂРµР» Р»СЋРґРµР№ РІ Р СѓР·Рµ РЅР° РіР»Р°Р·Р°С… Сѓ РїРѕР»РёС†РёРё РёР»Рё \"РЎС‚Р°РЅРёС†Р° РљСѓС‰РµРІСЃРєР°СЏ-2\" http://t.co/yltKP7IN","source":"\u003ca href\u003d\"http://www.livejournal.com/\" rel\u003d\"nofollow\"\u003eLiveJournal.com\u003c/a\u003e","isTruncated":false,"inReplyToStatusId":-1,"inReplyToUserId":-1,"isFavorited":false,"isRetweeted":false,"favoriteCount":0,"retweetCount":1,"isPossiblySensitive":false,"lang":"ru","contributorsIDs":[],"userMentionEntities":[],"urlEntities":[{"url":"http://t.co/yltKP7IN","expandedURL":"http://j.mp/AjxNbp","displayURL":"j.mp/AjxNbp","start":68,"end":88}],"hashtagEntities":[],"mediaEntities":[],"extendedMediaEntities":[],"symbolEntities":[],"currentUserRetweetId":-1},"userMentionEntities":[{"name":"RusStyle","screenName":"RussianStyle_","id":270033437,"start":3,"end":17}],"urlEntities":[{"url":"http://t.co/yltKP7IN","expandedURL":"http://j.mp/AjxNbp","displayURL":"j.mp/AjxNbp","start":87,"end":107}],"hashtagEntities":[],"mediaEntities":[],"extendedMediaEntities":[],"symbolEntities":[],"currentUserRetweetId":-1},"profileBackgroundColor":"C0DEED","profileTextColor":"333333","profileLinkColor":"0084B4","profileSidebarFillColor":"DDEEF6","profileSidebarBorderColor":"C0DEED","profileUseBackgroundImage":true,"isDefaultProfile":true,"showAllInlineMedia":false,"friendsCount":9,"createdAt":"Nov 4, 2011 11:50:07 AM","favouritesCount":2,"utcOffset":10800,"timeZone":"Moscow","profileBackgroundImageUrl":"http://abs.twimg.com/images/themes/theme1/bg.png","profileBackgroundImageUrlHttps":"https://abs.twimg.com/images/themes/theme1/bg.png","profileBackgroundTiled":false,"lang":"en","statusesCount":3,"isGeoEnabled":false,"isVerified":false,"translator":false,"listedCount":0,"isFollowRequestSent":false},{"id":1852359968,"name":"Yaroslav Nechaev","screenName":"CNechaev","location":"","description":"","descriptionURLEntities":[],"isContributorsEnabled":false,"profileImageUrl":"http://abs.twimg.com/sticky/default_profile_images/default_profile_4_normal.png","profileImageUrlHttps":"https://abs.twimg.com/sticky/default_profile_images/default_profile_4_normal.png","isDefaultProfileImage":true,"isProtected":false,"followersCount":1,"profileBackgroundColor":"C0DEED","profileTextColor":"333333","profileLinkColor":"0084B4","profileSidebarFillColor":"DDEEF6","profileSidebarBorderColor":"C0DEED","profileUseBackgroundImage":true,"isDefaultProfile":true,"showAllInlineMedia":false,"friendsCount":8,"createdAt":"Sep 10, 2013 9:49:46 PM","favouritesCount":0,"utcOffset":-1,"profileBackgroundImageUrl":"http://abs.twimg.com/images/themes/theme1/bg.png","profileBackgroundImageUrlHttps":"https://abs.twimg.com/images/themes/theme1/bg.png","profileBackgroundTiled":false,"lang":"ru","statusesCount":0,"isGeoEnabled":false,"isVerified":false,"translator":false,"listedCount":0,"isFollowRequestSent":false},{"id":94535753,"name":"nechaev yaroslav","screenName":"yan76","location":"","description":"","descriptionURLEntities":[],"isContributorsEnabled":false,"profileImageUrl":"http://abs.twimg.com/sticky/default_profile_images/default_profile_4_normal.png","profileImageUrlHttps":"https://abs.twimg.com/sticky/default_profile_images/default_profile_4_normal.png","isDefaultProfileImage":true,"isProtected":false,"followersCount":0,"profileBackgroundColor":"C0DEED","profileTextColor":"333333","profileLinkColor":"0084B4","profileSidebarFillColor":"DDEEF6","profileSidebarBorderColor":"C0DEED","profileUseBackgroundImage":true,"isDefaultProfile":true,"showAllInlineMedia":false,"friendsCount":1,"createdAt":"Dec 4, 2009 11:59:08 AM","favouritesCount":0,"utcOffset":-1,"profileBackgroundImageUrl":"http://abs.twimg.com/images/themes/theme1/bg.png","profileBackgroundImageUrlHttps":"https://abs.twimg.com/images/themes/theme1/bg.png","profileBackgroundTiled":false,"lang":"en","statusesCount":0,"isGeoEnabled":false,"isVerified":false,"translator":false,"listedCount":0,"isFollowRequestSent":false}];

@Component({
    selector: 'smt-profiles',
    templateUrl: '../../templates/profiles.html',
    providers: [ CandidatesService ]
})
export class Profiles {
    private rawCandidates = [];
    private rows;
    private status;
    private errorMessage;

    search(searchString : string, topic : string) {
        let self = this;
        self.status = "LOADING";
        self.rows = [];
        var func;
        if (topic.length == 0) {
            func = this.candidateService.getCandidates(searchString);
        } else {
            func = this.candidateService.getCandidatesWithTopic(searchString, topic);
        }
        func.subscribe(
            function(candidates) {
                self.rawCandidates = candidates;
                self.loadSearchResults()
            },
            function(error) {
                self.rawCandidates = [];
                self.status = "ERROR";
                self.errorMessage = <any>error;
            }
        )
    }

    private loadSearchResults() {
        this.rows = [];
        if (this.rawCandidates.length == 0) {
            this.status = "EMPTY";
            return;
        }
        if (typeof(this.rawCandidates[0].score) === 'undefined') {
            this.rawCandidates = this.rawCandidates.map(function(candidate) {
                return {
                    user: candidate,
                    score: 0.0
                }
            });
        }
        let curRow = [];
        for (let idx in this.rawCandidates) {
            curRow.push(this.rawCandidates[idx]);
            if (curRow.length == 3) {
                this.rows.push(curRow);
                curRow = [];
            }
        }
        if (curRow.length > 0) {
            this.rows.push(curRow);
        }
        this.status = "DONE";
    }

    constructor(private candidateService : CandidatesService) {
        this.status = "NONE";
        this.rawCandidates = CANDIDATES;
        this.loadSearchResults();
    };
}