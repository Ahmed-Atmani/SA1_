package Common

/**
 * A match record in the database
 * @param season year of the match
 * @param round  round of the match
 * @param days_from_epoch days from epoch
 * @param game_date date of the match
 * @param day day of the week
 * @param win_team winning team
 * @param lose_team losing team
 * @param win_pts points of winning team
 * @param lose_pts points of losing team
 * @param num_ot overtime points
 * @param academic_year academic year
 */
class Match(val season: Int,
            val round: Int,
            val days_from_epoch: Int,
            val game_date: String,
            val day: String,
            val win_team: Team,
            val lose_team: Team,
            val win_pts: Int,
            val lose_pts: Int,
            val num_ot: Int,
            val academic_year: Int) extends Serializable:
  override def toString: String = s"Match(${win_team.name} [$win_pts-$lose_pts] ${lose_team.name})\n"


/**
 * A team that is in the database
 * @param seed seed of the team
 * @param region region of the team
 * @param market market of the team
 * @param name team name
 * @param alias team name alias
 * @param team_id team id
 * @param school_ncaa school ncaa
 * @param code_ncaa code ncaa
 * @param kaggle_team_id kaggle team id
 */
class Team(val seed: Int,
           val region: String ,
           val market: String ,
           val name: String ,
           val alias: String ,
           val team_id: String ,
           val school_ncaa: String ,
           val code_ncaa: Int,
           val kaggle_team_id: Int) extends Serializable:
  override def toString: String = s"Team($name)\n"
